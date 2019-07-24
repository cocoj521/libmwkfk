#include "mwkfk_producer.h"
#include <strings.h>
#include <set>
#include <map>
#include <iostream>
#include "include/librdkafka/rdkafka.h"
#include "util/logger.h"
#include "mwkfk_constant.h"
#include "mwkfk_helper.h"
#include "mwkfk_config.h"
#include "mwkfk_record_msg.h"
#include "mwkfk_producer_imp.h"

extern "C" 
{
typedef  struct rd_kafka_broker_s rd_kafka_broker_t;    
rd_kafka_broker_t * rd_kafka_broker_any (rd_kafka_t *rk, int state,
												int (*filter) (rd_kafka_broker_t *rkb,
												void *opaque),
												void *opaque);
}

namespace mwkfk 
{

ProducerImpl::ProducerImpl():
broker_list_(""),
max_rd_kafka_outq_len_(RD_KAFKA_SDK_MAX_OUTQUE_SIZE),
is_init_(false),
delivered_cb_(NULL),
pInvoker_(NULL)
{
	kafka_handle_.reset(new tKafkaHandle());
}

ProducerImpl::~ProducerImpl() 
{
}

bool ProducerImpl::Init(const std::string& broker_list, const std::string& log_path, const std::string& config_path) 
{
	if (is_init_)
	{
		ERROR(__FUNCTION__ << " | has inited");
		return is_init_;
	}
	
	config_loader_.LoadConfig(config_path);

	MWkfkHelper::InitLog(config_loader_.GetSdkConfig(RD_KAFKA_SDK_CONFIG_LOG_LEVEL, RD_KAFKA_SDK_CONFIG_LOG_LEVEL_DEFAULT), log_path);

	INFO(__FUNCTION__ 
		<< " | Start init | kafka cluster: " << broker_list
		<< " | log: "  << log_path
		<< " | config: " << config_path);
	
	broker_list_ = broker_list;
	
	MWkfkHelper::GetMWkfkBrokerList(config_loader_, &broker_list_); 
	INFO(__FUNCTION__ << " | broker list:" << broker_list_);

	KafkaHandlePtr kafka_handle(new tKafkaHandle());	
	kafka_handle->conf_ = InitRdKafkaConfig();
	kafka_handle->handle_ = InitRdKafkaHandle(kafka_handle->conf_);
	if (NULL != kafka_handle->handle_)
	{
		if (rd_kafka_brokers_add(kafka_handle->handle_ , broker_list_.c_str()) <= 0) 
		{
			ERROR(__FUNCTION__ << " | Failed to rd_kafka_broker_add | broker list:" << broker_list_);
		}
		else
		{
			kafka_handle_ = kafka_handle;
			is_init_ = true;
		}
	}
	
	return is_init_;
}

void ProducerImpl::UnInit(int timeout_ms) 
{
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		return;
	}
	
	INFO(__FUNCTION__ << " | Startting uninit...");
	
	KafkaHandlePtr pKafka = kafka_handle_;
	if (nullptr != pKafka && NULL != pKafka->handle_)
	{
		if (timeout_ms <= 0)
		{
			//保证退出前所有队列中的数据全部发送完毕
			while (rd_kafka_outq_len(pKafka->handle_) > 0) 
			{
				//at regular
				//rd_kafka_poll(pKafka->handle_, RD_KAFKA_POLL_TIMIE_OUT_MS);  
				//before terminating
				if (RD_KAFKA_RESP_ERR_NO_ERROR == rd_kafka_flush(pKafka->handle_, RD_KAFKA_SDK_FORCE_KAFKA_FLUSH_TIMEOUT_MAX))  
				{
					break;
				}
				const char* err = rd_kafka_err2str(rd_kafka_last_error());
				INFO(__FUNCTION__ 
				<< " | Stopping...:"
				<< " | outque: " << rd_kafka_outq_len(pKafka->handle_)
				<< " | errmsg: " << (err ? err : "unknown err"));
			}

			const char* err = rd_kafka_err2str(rd_kafka_last_error());
			INFO(__FUNCTION__ 
			<< " | Stopped"
			<< " | outque: " << rd_kafka_outq_len(pKafka->handle_)
			<< " | errmsg: " << (err ? err : "unknown err"));
		}
		else
		{
			//等待固定时间,不保存证所有数据一定全部发送完毕
			if (rd_kafka_outq_len(pKafka->handle_) > 0)
			{
				//at regular
				//rd_kafka_poll(pKafka->handle_, timeout_ms);  
				//before terminating
				if (RD_KAFKA_RESP_ERR__TIMED_OUT == rd_kafka_flush(pKafka->handle_, timeout_ms))   
				{
					//记录日志,记录可能丢掉的数量
					const char* err = rd_kafka_err2str(rd_kafka_last_error());
					INFO(__FUNCTION__ 
					<< " | Stop timeout:" << timeout_ms
					<< " | outque: " << rd_kafka_outq_len(pKafka->handle_)
					<< " | errmsg: " << (err ? err : "unknown err"));
				}
			}
		}
		
		kafka_topics_.clear();
		kafka_handle_.reset();		
	}

	rd_kafka_wait_destroyed(timeout_ms);

	if (is_init_) is_init_ = false;

	INFO(__FUNCTION__ << " | Finished uninit");
}

//通知RDKAFKA强制将缓存中的信息发送到KAFKA
void ProducerImpl::Stop(int timeout_ms)
{
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		return;
	}
	
	INFO(__FUNCTION__ << " | Startting Stop...");

	KafkaHandlePtr pKafka = kafka_handle_;
	if (nullptr != pKafka && NULL != pKafka->handle_)
	{
		if (timeout_ms <= 0)
		{
			//保证退出前所有队列中的数据全部发送完毕
			while (rd_kafka_outq_len(pKafka->handle_) > 0) 
			{
				//at regular
				//rd_kafka_poll(pKafka->handle_, RD_KAFKA_POLL_TIMIE_OUT_MS);  
				//before terminating
				if (RD_KAFKA_RESP_ERR_NO_ERROR == rd_kafka_flush(pKafka->handle_, RD_KAFKA_SDK_FORCE_KAFKA_FLUSH_TIMEOUT_MAX))  
				{
					break;
				}
				const char* err = rd_kafka_err2str(rd_kafka_last_error());
				INFO(__FUNCTION__ 
				<< " | Stopping...:"
				<< " | outque: " << rd_kafka_outq_len(pKafka->handle_)
				<< " | errmsg: " << (err ? err : "unknown err"));
			}

			const char* err = rd_kafka_err2str(rd_kafka_last_error());
			INFO(__FUNCTION__ 
			<< " | Stopped"
			<< " | outque: " << rd_kafka_outq_len(pKafka->handle_)
			<< " | errmsg: " << (err ? err : "unknown err"));
		}
		else
		{
			//等待固定时间,不保存证所有数据一定全部发送完毕
			if (rd_kafka_outq_len(pKafka->handle_) > 0)
			{
				//at regular
				//rd_kafka_poll(pKafka->handle_, timeout_ms);  
				//before terminating
				if (RD_KAFKA_RESP_ERR__TIMED_OUT == rd_kafka_flush(pKafka->handle_, timeout_ms))   
				{
					//记录日志,记录可能丢掉的数量
					const char* err = rd_kafka_err2str(rd_kafka_last_error());
					INFO(__FUNCTION__ 
					<< " | Stop timeout:" << timeout_ms
					<< " | outque: " << rd_kafka_outq_len(pKafka->handle_)
					<< " | errmsg: " << (err ? err : "unknown err"));
				}
			}
		}
	}
	
	INFO(__FUNCTION__ << " | Finished Stop");
}

//通知RDKAFKA将抓紧投递消息到KAFKA
void ProducerImpl::Poll(int timeout_ms)
{
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		usleep(timeout_ms*1000);
		return;
	}
	
	KafkaHandlePtr pKafka = kafka_handle_;
	if (nullptr != pKafka && NULL != pKafka->handle_)
	{
		timeout_ms<0?timeout_ms=0:1;
		rd_kafka_poll(pKafka->handle_, timeout_ms);
	}
	else
	{
		usleep(timeout_ms*1000);
	}
}

//增加新的broker
//如果kafka集群中新增加了结点,可以调用该函数动态增加
//格式:
//"broker1 ip:port,broker2_ip:port,..."
//"SSL://broker3_ip:port,ssl://broker4_ip:port,..."
//返回值:添加成功的broker的个数
int ProducerImpl::AddNewBrokers(const char* brokers)
{
	int cnt = 0;
	
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		return cnt;
	}
	
	KafkaHandlePtr pKafka = kafka_handle_;
	if (nullptr != pKafka && NULL != pKafka->handle_)
	{
		cnt = rd_kafka_brokers_add(pKafka->handle_, brokers);
		rd_kafka_poll(pKafka->handle_, 0);
		if (cnt > 0)
		{
			broker_list_ += ",";
			broker_list_ += brokers;
			
			INFO(__FUNCTION__ << " | add broker: " << brokers << " success,all brokers: " << broker_list_);
		}
		else
		{
			cnt = 0;
			
			ERROR(__FUNCTION__ << " | add broker: " << brokers << " fail,errmsg: " << rd_kafka_err2str(rd_kafka_last_error()));
		}
	}
	else
	{
		ERROR(__FUNCTION__ << " | kafka handle is null");
	}

	return cnt;

}

//返回RDKAFKA当前待发队列的大小
size_t ProducerImpl::GetOutQueSize() const
{
	size_t cnt = 0;
	
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		return cnt;
	}
	
	KafkaHandlePtr pKafka = kafka_handle_;
	if (nullptr != pKafka && NULL != pKafka->handle_)
	{
		cnt = rd_kafka_outq_len(pKafka->handle_);
	}	

	return cnt;
}

//设置最大允许RDKAFKA OUTQUE的大小
void ProducerImpl::SetMaxOutQueSize(size_t size)
{
	max_rd_kafka_outq_len_ = size;
}

bool ProducerImpl::InternalProduce(rd_kafka_topic_t* kafka_topic, const char* data, size_t data_len, const char* key, size_t key_len, void *msg_opaque, std::string& errmsg) 
{
	bool rt = false;

	KafkaHandlePtr pKafka = kafka_handle_;
	if (nullptr != pKafka && NULL != pKafka->handle_ && NULL != kafka_topic)
	{
		//如果超过设定的队列大小,通知rdkafka将消息发往kafka,并等待一段时间,若在等待期间一直未投递完,则超时返回
		int loop_cnt = RD_KAFKA_SYNC_SEND_POLL_TIME;
		while (rd_kafka_outq_len(pKafka->handle_) >= max_rd_kafka_outq_len_ && --loop_cnt >= 0)
		{
			DEBUG(__FUNCTION__ << " | outque len > " << max_rd_kafka_outq_len_ << " wait...");
			rd_kafka_poll(pKafka->handle_, RD_KAFKA_PRODUCE_SYNC_SEND_POLL_TIMEOUT_MS);
		}
		/*
		tMsgPrivateData* pPrivate = static_cast<tMsgPrivateData*>(msg_opaque);
		if (NULL != pPrivate) 
		{
			//更新消息生产时间
			pPrivate->in_tm_ms_  = MWkfkHelper::GetCurrentTimeMs();
		}
		*/
		const char* pKey = ((key_len<=0||NULL==key)?NULL:key);
		size_t nKeyLen   = ((key_len<=0||NULL==key)?0:key_len);
		int ret = rd_kafka_produce(kafka_topic,
							RD_KAFKA_PARTITION_UA,
							RD_KAFKA_MSG_PASSTHROUGH,
							//RD_KAFKA_MSG_F_FREE,
							//RD_KAFKA_MSG_F_COPY,
							static_cast<void*>(const_cast<char*>(data)),
							data_len,
							static_cast<void*>(const_cast<char*>(pKey)),
							nKeyLen,
							msg_opaque);
		if (0 != ret)
		{
			const char* topic = rd_kafka_topic_name(kafka_topic);
			const char* err = rd_kafka_err2str(rd_kafka_last_error());
			int outq_len = rd_kafka_outq_len(pKafka->handle_);
			errmsg = (err ? err : "rd_kafka_produce unknown err");
			
			ERROR(__FUNCTION__ << " | Failed to produce"
			<< " | topic: " << (topic ? topic : "unknown topic")
			<< " | ret: " << ret 
			<< " | outque: " << outq_len
			<< " | msg error: " << errmsg);
		}
		else
		{
			rt = true;

			errmsg = "success";
		}
		
		// 通知rdkafka及时提交信息到kafka
		rd_kafka_poll(pKafka->handle_, 0);
	}
	else
	{
		errmsg = "rdkafka handle ptr error";	
	}
	
	return rt;
}

//生产消息
bool ProducerImpl::Produce(const ProduceMessagePtr& msg_ptr, const tMsgPrivateData* pPrivate, std::string& errmsg) 
{
	bool rt = false;
	
	if (!is_init_)
	{
		errmsg = "has not inited";
		
		ERROR(__FUNCTION__ << " | has not inited");
		return rt;
	}
	
	KafkaTopicPtr pTopic = GetTopic(msg_ptr->topic.c_str());

	if (nullptr != pTopic) 
	{
		rt = InternalProduce(pTopic->topic_, 
							msg_ptr->data.c_str(), msg_ptr->data.size(), 
							msg_ptr->key.c_str(), msg_ptr->key.size(), 
							static_cast<void*>(const_cast<tMsgPrivateData*>(pPrivate)), 
							errmsg);
	}
	else
	{
		errmsg = "topic has not added";
		
		ERROR(__FUNCTION__ << " | Failed to produce:null topic ptr");
	}
	
	return rt;
}

//初始化rdkafka句柄
rd_kafka_t* ProducerImpl::InitRdKafkaHandle(rd_kafka_conf_t* kafka_conf) 
{	
	char err_str[512] = {0};
	
	rd_kafka_t* kafka_handle = rd_kafka_new(RD_KAFKA_PRODUCER,
											kafka_conf,
											err_str,
											sizeof(err_str));
	if (NULL == kafka_handle)
	{
		ERROR(__FUNCTION__ << " | Failed to create new producer | error msg: " << err_str);
	} 
	
	return kafka_handle;
}

//添加需要生产的topic
bool ProducerImpl::AddTopic(const std::string& topic_name)
{
	bool rt = false;

	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		return rt;
	}
	
	KafkaTopicPtr pTopic(new tKafkaTopic());
	pTopic->conf_ = InitRdKafkaTopicConfig();
	if (pTopic->conf_)
	{
		pTopic->topic_ = InitRdKafkaTopic(topic_name, kafka_handle_->handle_, pTopic->conf_);
		if (NULL != pTopic->topic_)
		{
			std::lock_guard<std::mutex> lock(lock_topic);
			kafka_topics_.insert(std::make_pair(topic_name, pTopic));

			rt = true;
		}
		else
		{
			ERROR(__FUNCTION__ << " | Filed to AddTopic: " << topic_name << " | null topic ptr");
		}
	}
	else
	{
		ERROR(__FUNCTION__ << " | Filed to AddTopic: " << topic_name << " | null topic_conf ptr");
	}
	
	return rt;
}

//根据topic名称获取rdkafak_topic句柄
KafkaTopicPtr ProducerImpl::GetTopic(const std::string& topic_name)
{
	KafkaTopicPtr pTopic = nullptr;
	
	std::lock_guard<std::mutex> lock(lock_topic);
	auto it = kafka_topics_.find(topic_name);
	if (it != kafka_topics_.end())
	{
		pTopic = it->second;
	}

	return pTopic;
}

//初始化rdkafak_topic句柄
rd_kafka_topic_t* ProducerImpl::InitRdKafkaTopic(const std::string& topic_name, rd_kafka_t* kafka_handle, rd_kafka_topic_conf_t* topic_conf) 
{
	rd_kafka_topic_t* kafka_topic = rd_kafka_topic_new(kafka_handle, topic_name.c_str(), topic_conf);
	
	if (NULL == kafka_topic) 
	{
		const char* err = rd_kafka_err2str(rd_kafka_last_error());
		
		ERROR(__FUNCTION__ << " | Failed to rd_kafka_topic_new | error msg: " << (err ? err : "unknown error"));
	} 

	return kafka_topic;
}

//2.设置消息投递结果回调函数
void ProducerImpl::SetDeliveredCallBack(void* pInvoker, pfunc_on_msgdelivered pOnDeliveredCb)
{
	pInvoker_ = pInvoker;
	delivered_cb_ = pOnDeliveredCb;
}

/**
 * @brief Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll() and executes on
 * the application's thread.
 */
//消息投递回调函数
void ProducerImpl::MsgDeliveredCallback(rd_kafka_t *rk, const rd_kafka_message_t *rk_msg, void *opaque) 
{	
	//指针为空或private为空,则丢弃....
	if (NULL == rk) 
	{
		ERROR(__FUNCTION__ << " | rd_kafka_t ptr is null");
		return;
	}

	if (NULL == rk_msg) 
	{
		ERROR(__FUNCTION__ << " | rd_kafka_message_t ptr is null");
		return;
	}
	
	tMsgPrivateData* pPrivate = static_cast<tMsgPrivateData*>(rk_msg->_private);
	if (NULL == pPrivate) 
	{
		ERROR(__FUNCTION__ << " | Failed to static_cast from rk_msg->_private to tMsgPrivateData");
		return;
	}
	//用智能指针保存,以防漏析构
	std::shared_ptr<tMsgPrivateData> sptr_private(pPrivate);
	
	ProducerImpl* pProducer = static_cast<ProducerImpl*>(opaque);
	if (NULL == pProducer) 
	{
		ERROR(__FUNCTION__ << " | Failed to static_cast from opaque to ProducerImpl");
		return;
	}

	//还原produce消息
	ProduceMessagePtr pMsg(boost::any_cast<ProduceMessagePtr>(pPrivate->any_)); 
	if (nullptr == pMsg)
	{
		DEBUG(__FUNCTION__<< " | cast private->any_ to ProduceMessagePtr fail");
		return;
	}
	
	//获取消息状态等信息
	pMsg->status = rd_kafka_message_status(rk_msg);	
	pMsg->errcode =  rk_msg->err;
	pMsg->errmsg = rd_kafka_err2str(rk_msg->err);
	pMsg->partition = rk_msg->partition;
	pMsg->offset = rk_msg->offset;
	pMsg->in_tm_ms = sptr_private->in_tm_ms_;
	pMsg->cb_tm_ms = MWkfkHelper::GetCurrentTimeMs();
	
	//失败....(err不为0,或状态不为PERSISTED,均视为失败)
	//if ((RD_KAFKA_RESP_ERR_NO_ERROR != rkmessage->err && NULL != rd_kafka_broker_any(rk, 4, NULL, NULL))  
	if (RD_KAFKA_RESP_ERR_NO_ERROR != pMsg->errcode || RD_KAFKA_MSG_STATUS_PERSISTED != pMsg->status)
	{  
		ERROR(__FUNCTION__ 
			<< " | Failed to delivery message | err msg: " << pMsg->errmsg
			<< " | topic: " << pMsg->topic
			<< " | partition: " << pMsg->partition
			<< " | offset: " << pMsg->offset
			<< " | msgstatus: " << pMsg->status
			<< " | msglen: " << pMsg->data.size()
			<< " | msg: " << pMsg->data
			<< " | keylen: " << pMsg->key.size()
			<< " | key: " << pMsg->key);
	} 
	else 
	{				
		DEBUG(__FUNCTION__ 
			<< " Successed to delivery message"
			<< " | topic: " << pMsg->topic
			<< " | partition: " << pMsg->partition
			<< " | offset: " << pMsg->offset
			<< " | msgstatus: " << pMsg->status
			<< " | msglen: " << pMsg->data.size()
			<< " | msg: " << pMsg->data
			<< " | keylen: " << pMsg->key.size()
			<< " | key: " << pMsg->key);

	}
	
	//将成果的结果回调给上层
	if (pProducer->delivered_cb_)
	{
		pProducer->delivered_cb_(pProducer->pInvoker_, pMsg);
	}
	else
	{
		WARNING(__FUNCTION__<< " | delivered_cb_ ptr is null");
	}
}

//djb hash算法
inline unsigned int djb_hash (const char *str, size_t len) 
{
	unsigned int hash = 5381;
	for (size_t i = 0 ; i < len ; i++)
	{
		hash = ((hash << 5) + hash) + str[i];
	}
	return hash;
}

//分区选择算法
int32_t ProducerImpl::partitioner_cb(const rd_kafka_topic_t *rkt,
									            const void *keydata,
									            size_t keylen,
									            int32_t partition_cnt,
									            void *rkt_opaque,
									            void *msg_opaque) 
{
	int32_t hit_partition = 0;

	//key有值,取hash;key没值,随机
	if (keylen > 0 && NULL != keydata) 
	{
		const char* key = static_cast<const char*>(keydata);
		hit_partition = djb_hash(key, keylen) % partition_cnt;
	}
	else
	{
		hit_partition = rd_kafka_msg_partitioner_random(rkt,
														keydata,
														keylen,
														partition_cnt,
														rkt_opaque,
														msg_opaque);
	}

	//检查分区是否可用,如果不可用,取出所有可用分区,然后再随机取一个
	if (1 != rd_kafka_topic_partition_available(rkt, hit_partition)) 
	{
		DEBUG(__FUNCTION__ << " | select parition not avaiable | current invailed partition: " << hit_partition);
		
		int32_t i = 0;
		std::vector<int32_t> vPartition;
		for (; i < partition_cnt; ++i)
		{
			if (0 == rd_kafka_topic_partition_available(rkt, hit_partition)) 
			{
				vPartition.push_back(i);
			}
		}
		size_t nSize = vPartition.size();
		if (nSize > 0)
		{
			srand(time(NULL));
			int nRand = MIN(rand() % nSize, nSize - 1);
			hit_partition = vPartition[nRand];
		}
		else
		{
			hit_partition = 0;
		}
	}
	
	DEBUG(__FUNCTION__ << " | hit_partition:" << hit_partition);

	return hit_partition;
}

rd_kafka_conf_t* ProducerImpl::InitRdKafkaConfig() 
{
	INFO(__FUNCTION__ << " | Librdkafka version: " << rd_kafka_version_str() << " " << rd_kafka_version());

	rd_kafka_conf_t* rd_kafka_conf = rd_kafka_conf_new();
	if (NULL != rd_kafka_conf) 
	{
		rd_kafka_conf_set_opaque(rd_kafka_conf, static_cast<void*>(this));
					
		rd_kafka_conf_set_dr_msg_cb(rd_kafka_conf, &ProducerImpl::MsgDeliveredCallback);
		
		if (!config_loader_.IsSetConfig(RD_KAFKA_CONFIG_TOPIC_METADATA_REFRESH_INTERVAL, false)) 
		{
			MWkfkHelper::SetRdKafkaConfig(rd_kafka_conf,
										RD_KAFKA_CONFIG_TOPIC_METADATA_REFRESH_INTERVAL,
										RD_KAFKA_CONFIG_TOPIC_METADATA_REFRESH_INTERVAL_MS);
		}

		std::string minimize_producer_latency = 
		config_loader_.GetSdkConfig(RD_KAFKA_SDK_CONFIG_PRODUCER_ENABLE_MINI_LATENCY,
									RD_KAFKA_SDK_CONFIG_PRODUCER_ENABLE_MINI_LATENCY_DEFAULT); 
		if (0 != strncasecmp(minimize_producer_latency.c_str(), RD_KAFKA_SDK_CONFIG_PRODUCER_ENABLE_MINI_LATENCY_DEFAULT, minimize_producer_latency.length())) 
		{
			DEBUG(__FUNCTION__ << " | enable minimize producer latency");
			MWkfkHelper::SetRdKafkaConfig(rd_kafka_conf, 
										RD_KAFKA_CONFIG_QUEUE_BUFFERING_MAX_MS, 
										RD_KAFKA_SDK_MINIMIZE_PRODUCER_LATENCY_VALUE);
			MWkfkHelper::SetRdKafkaConfig(rd_kafka_conf,
										RD_KAFKA_CONFIG_SOCKET_BLOKING_MAX_MX,
										RD_KAFKA_SDK_MINIMIZE_PRODUCER_LATENCY_VALUE);
		}
		
		config_loader_.LoadRdkafkaConfig(rd_kafka_conf, NULL);
	} 
	else 
	{
		ERROR(__FUNCTION__ << " | Failed to rd_kafka_conf_new");
	}

	return rd_kafka_conf;
}

rd_kafka_topic_conf_t* ProducerImpl::InitRdKafkaTopicConfig() 
{
	INFO(__FUNCTION__ << " | Librdkafka version: " << rd_kafka_version_str() << " " << rd_kafka_version());

	rd_kafka_topic_conf_t* topic_conf = rd_kafka_topic_conf_new();
	if (NULL != topic_conf)
	{
		//设置分区分配策略函数,也可不做hash,让kafka自已选择 
		rd_kafka_topic_conf_set_partitioner_cb(topic_conf, &ProducerImpl::partitioner_cb);
		
		config_loader_.LoadRdkafkaConfig(NULL, topic_conf);
	} 
	else 
	{
		ERROR(__FUNCTION__ << " | Failed to rd_kafka_topic_conf_new");
	}

	return topic_conf;
}

///////////////////////////////////////////////////////////////////////////
Producer::Producer()
{
	producer_impl_.reset(new ProducerImpl());
}

Producer::~Producer() 
{
}

bool Producer::Init(const std::string& broker_list, const std::string& log_path, const std::string& config_path) 
{
	bool rt = false;

	if (nullptr != producer_impl_) 
	{
		rt = producer_impl_->Init(broker_list, log_path, config_path);
		if (rt) 
		{
			INFO(__FUNCTION__ << " | Procuder init is OK!");
		} 
		else 
		{
			ERROR(__FUNCTION__ << " | Failed to init");
		}
	}

	return rt;
}

void Producer::UnInit(int timeout_ms) 
{
	if (nullptr != producer_impl_) 
	{
		producer_impl_->UnInit(timeout_ms);
	}
}

//2.设置消息投递结果回调函数
void Producer::SetDeliveredCallBack(void* pInvoker, pfunc_on_msgdelivered pOnDeliveredCb)
{
	if (nullptr != producer_impl_) 
	{
		producer_impl_->SetDeliveredCallBack(pInvoker, pOnDeliveredCb);
	}
}

//添加需要生产的topic
bool Producer::AddTopic(const std::string& topic)
{
	bool rt = false;
	
	if (nullptr != producer_impl_) 
	{
		rt = producer_impl_->AddTopic(topic);
	}

	return rt;
}

void Producer::Stop(int timeout_ms)
{
	if (nullptr != producer_impl_) 
	{
		producer_impl_->Stop(timeout_ms);
	}
}

bool Producer::Produce(const ProduceMessagePtr& msg_ptr, std::string& errmsg) 
{
	bool rt = false;

	errmsg = "success";
	
	if (!msg_ptr->data.empty() && nullptr != producer_impl_) 
	{
		DEBUG(__FUNCTION__ << " | topic: " << msg_ptr->topic << " | msg: " << msg_ptr->data << " | key: " << msg_ptr->key);

		tMsgPrivateData* pPrivate = new tMsgPrivateData();
		if (NULL != pPrivate)
		{
			pPrivate->any_ = msg_ptr;
			pPrivate->in_tm_ms_  = MWkfkHelper::GetCurrentTimeMs();
			pPrivate->retry_cnt_ = 0;
			rt = producer_impl_->Produce(msg_ptr, pPrivate, errmsg);
		}
		else
		{
			errmsg = "new private data fail";
		}
	} 
	else 
	{
		ERROR(__FUNCTION__ << " | Failed to produce | data is null" << " | data len: " << msg_ptr->data.size());
		
		errmsg = "invalid data & data_len";
	}

	return rt;
}

//4.定时调用,以便通知底层让消息抓紧投递到kafka
//timeout_ms=0仅代表通知,立刻返回,>0会阻塞
void Producer::Poll(int timeout_ms)
{
	if (nullptr != producer_impl_)
	{
		producer_impl_->Poll(timeout_ms);
	}
	else
	{
		usleep(timeout_ms*1000);
	}
}

//返回RDKAFKA当前待发队列的大小
size_t Producer::GetOutQueSize() const
{
	size_t cnt = 0;
	
	if (nullptr != producer_impl_)
	{
		cnt = producer_impl_->GetOutQueSize();
	}

	return cnt;
}

//设置最大允许RDKAFKA OUTQUE的大小
void Producer::SetMaxOutQueSize(size_t size)
{
	if (nullptr != producer_impl_)
	{
		producer_impl_->SetMaxOutQueSize(size);
	}
}

int Producer::AddNewBrokers(const char* brokers)
{
	int cnt = 0;
	
	if (nullptr != producer_impl_)
	{
		cnt = producer_impl_->AddNewBrokers(brokers);
	}

	return cnt;
}

static __attribute__((destructor)) void end() 
{
	LUtil::Logger::uninit();
}

} //namespace mwkfk
