#include "mwkfk_consumer_imp.h"

#include <strings.h>
#include <string.h>
#include <errno.h>
#include <iostream>
#include <pthread.h>
#include "util/logger.h"
#include "mwkfk_constant.h"
#include "mwkfk_helper.h"

#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Wold-style-cast"

namespace mwkfk 
{
ConsumerImpl::ConsumerImpl(    ):
  kafka_handle_(nullptr),
  is_init_(false),
  max_wait_commit_(10000),
  commit_freq_(200),
  consumed_cb_(NULL),
  offsetcommitted_cb_(NULL),
  pInvoker_(NULL),
  last_commit_tm_(0)
{
}

ConsumerImpl::~ConsumerImpl() 
{
}

bool ConsumerImpl::Init(const std::string& group, const std::string& broker_list, const std::string& log_path, const std::string& config_path) 
{	
	if (is_init_)
	{
		ERROR(__FUNCTION__ << " | has inited");
		return is_init_;
	}
	if (group.empty())
	{
		ERROR(__FUNCTION__ << " | group is empty");
		
		is_init_ = false;
		return is_init_;
	}
  	config_loader_.LoadConfig(config_path);
	
  	MWkfkHelper::InitLog(config_loader_.GetSdkConfig(RD_KAFKA_SDK_CONFIG_LOG_LEVEL, RD_KAFKA_SDK_CONFIG_LOG_LEVEL_DEFAULT), log_path);

  	INFO(__FUNCTION__ 
		<< " | Start init | kafka cluster: " << broker_list
		<< " | log: "  << log_path
		<< " | config: " << config_path);
	
	broker_list_ = broker_list;
	group_ = group;
	
	MWkfkHelper::GetMWkfkBrokerList(config_loader_, &broker_list_); 
	INFO(__FUNCTION__ << " | broker list:" << broker_list_);

	rd_kafka_conf_t* rd_kafka_conf = NULL;
	if (InitRdKafkaConfig(&rd_kafka_conf) && InitRdKafkaHandle(rd_kafka_conf))
	{
		if (rd_kafka_brokers_add(kafka_handle_->handle_ , broker_list_.c_str()) <= 0) 
		{
			ERROR(__FUNCTION__ << " | Failed to rd_kafka_broker_add | broker list: " << broker_list_);

			is_init_ = false;
		}
		else
		{
			INFO(__FUNCTION__ << " | rd_kafka_brokers_add success: " << broker_list_);
			
			//创建消费者
			rd_kafka_resp_err_t resp_rs = rd_kafka_poll_set_consumer(kafka_handle_->handle_);
	  		if (RD_KAFKA_RESP_ERR_NO_ERROR == resp_rs) 
			{
				INFO(__FUNCTION__ << " | rd_kafka_poll_set_consumer success");
				
				is_init_ = true;
			}
			else
			{
				ERROR(__FUNCTION__ << " | Failed to rd_kafka_poll_set_consumer | err msg:" << rd_kafka_err2str(resp_rs));

				is_init_ = false;
			}
		}
		
	}
	
	return is_init_;
} 

bool ConsumerImpl::InitRdKafkaConfig(rd_kafka_conf_t** p_rd_kafka_conf) 
{
	bool rt = false;
	
	INFO(__FUNCTION__ << " | Librdkafka version: " << rd_kafka_version_str() << " " << rd_kafka_version());

	rd_kafka_conf_t* rd_kafka_conf = rd_kafka_conf_new();
	if (NULL != rd_kafka_conf) 
	{
		rd_kafka_conf_set_opaque(rd_kafka_conf, static_cast<void*>(this));

		rd_kafka_conf_set_rebalance_cb(rd_kafka_conf, &ConsumerImpl::rdkafka_rebalance_cb);

		//rd_kafka_conf_set_consume_cb(rd_kafka_conf, &ConsumerImpl::consume_cb);

		rd_kafka_conf_set_offset_commit_cb(rd_kafka_conf, &ConsumerImpl::rdkafka_offset_commit_cb);
		
		//manual commit(enable.auto.commit=false)
		MWkfkHelper::SetRdKafkaConfig(rd_kafka_conf, RD_KAFKA_CONFIG_ENABLE_AUTO_COMMIT, "false");

		//groupid 设置消费分组
		MWkfkHelper::SetRdKafkaConfig(rd_kafka_conf, RD_KAFKA_CONFIG_GROUP_ID, group_.c_str());
	
		//set client.id
		MWkfkHelper::SetClientId(rd_kafka_conf);

		config_loader_.LoadRdkafkaConfig(rd_kafka_conf, NULL);

		rd_kafka_topic_conf_t* rd_kafka_topic_conf = rd_kafka_topic_conf_new();
		if (NULL != rd_kafka_topic_conf) 
		{
			MWkfkHelper::SetRdKafkaTopicConfig(rd_kafka_topic_conf, 
											RD_KAFKA_TOPIC_CONFIG_AUTO_OFFSET_RESET, 
											RD_KAFKA_TOPIC_CONFIG_AUTO_OFFSET_RESET_EARLIEST);

			MWkfkHelper::SetRdKafkaTopicConfig(rd_kafka_topic_conf,
											RD_KAFKA_TOPIC_CONFIG_OFFSET_STORED_METHOD,
											RD_KAFKA_TOPIC_CONFIG_OFFSET_STORED_METHOD_BROKER);

			config_loader_.LoadRdkafkaConfig(NULL, rd_kafka_topic_conf);
			
			rd_kafka_conf_set_default_topic_conf(rd_kafka_conf, rd_kafka_topic_conf);

			rd_kafka_topic_conf_destroy(rd_kafka_topic_conf);

			*p_rd_kafka_conf = rd_kafka_conf;
			
			rt = true;
		}
		else
		{
			ERROR(__FUNCTION__ << " | rd_kafka_topic_conf_new fail | errmsg: " << rd_kafka_err2str(rd_kafka_last_error()));

			rd_kafka_conf_destroy(rd_kafka_conf);

			*p_rd_kafka_conf = NULL;

			rt = false;
		}
	}
	else
	{
		ERROR(__FUNCTION__ << " | rd_kafka_conf_new fail | errmsg: " << rd_kafka_err2str(rd_kafka_last_error()));

		*p_rd_kafka_conf = NULL;

		rt = false;
	}
	
	return rt;
}

bool ConsumerImpl::InitRdKafkaHandle(rd_kafka_conf_t* rd_kafka_conf) 
{
	bool rt = true;

	if (NULL != rd_kafka_conf)
	{
		char err_str[512] = {0};
		
		rd_kafka_t* rd_kafka_handle = rd_kafka_new(RD_KAFKA_CONSUMER, 
													rd_kafka_conf,
													err_str,
													sizeof(err_str));
		if (NULL == rd_kafka_handle)
		{
			ERROR(__FUNCTION__ << " | Failed to create new consumer | error msg:" << err_str);

			rt = false;
		}
		else
		{
			DEBUG(__FUNCTION__ << " | rd_kafka_new success");

			KafkaHandlePtr pKafka(new tKafkaHandle());
			pKafka->handle_ = rd_kafka_handle;
			kafka_handle_ = pKafka;
			
			rt = true;
		}
	}
	else
	{
		ERROR(__FUNCTION__ << " | rd_kafka_conf is null");
		
		rt = false;
	}
	
	return rt;
}

void ConsumerImpl::SetOffsetCommitPolicy(size_t max_wait_commit, int commit_freq)
{
	max_wait_commit_ = max_wait_commit;
	commit_freq_ = commit_freq;
}

void ConsumerImpl::SetConsumedCallBack(void* pInvoker, pfunc_on_msgconsumed pConsumeCb)
{
	pInvoker_ = pInvoker;
	consumed_cb_ = pConsumeCb;
}

void ConsumerImpl::SetOffsetCommitCallBack(void* pInvoker, pfunc_on_offsetcommitted pOffsetCommitted)
{
	pInvoker_ = pInvoker;
	offsetcommitted_cb_ = pOffsetCommitted;
}

bool ConsumerImpl::AddTopic(const topic_info_t& topic_info)
{
	bool rt = false;
	
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		return rt;
	}

	//判断是否已订阅
	if (IsSubscribed(topic_info.topic))
	{
		WARNING(__FUNCTION__ << " | has subscribed: " << topic_info.topic);
		rt = true;
		return rt;
	}

	//先取消订阅,再重新订阅
	KafkaHandlePtr pKafka = kafka_handle_;
	if (nullptr != pKafka && NULL != pKafka->handle_)
	{
		rd_kafka_resp_err_t err = rd_kafka_unsubscribe(pKafka->handle_);
		if (RD_KAFKA_RESP_ERR_NO_ERROR == err)
		{
			INFO(__FUNCTION__ << " | unsubscribe success");

			//这里要加锁,防止重复调用
			std::lock_guard<std::mutex> lock(topics_mutex_);
			topics_.insert(std::make_pair(topic_info.topic, topic_info));

			//订阅失败,清空所有已订阅topic
			std::string errmsg = "";
			if (!(rt = InternalSubscribe(topics_, errmsg)))
			{
				topics_.clear();
				
				ERROR(__FUNCTION__ << " | resubscribe fail");
			}
			else
			{
				INFO(__FUNCTION__ << " | resubscribe success");
			}
		}
		else
		{
			ERROR(__FUNCTION__ << " | unsubscribe fail,errmsg: " << rd_kafka_err2str(err));
		}
	}
	else
	{
		ERROR(__FUNCTION__ << " | kafka handle is null");
	}
	
	return rt;
}

bool ConsumerImpl::Pause(const std::string& topic)
{
	bool rt = false;
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		return rt;
	}

	INFO(__FUNCTION__ << " | Pause topic: " << topic);
	
	rt = PauseOrResumeConsume(topic_info_t(topic), false);

	INFO(__FUNCTION__ << " | Pause topic: " << topic << (rt?" success":" fail"));
	
	return rt;
}

bool ConsumerImpl::Resume(const std::string& topic)
{
	bool rt = false;
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		return rt;
	}

	INFO(__FUNCTION__ << " | Resume topic: " << topic);
	
	rt = PauseOrResumeConsume(topic_info_t(topic), true);

	INFO(__FUNCTION__ << " | Resume topic: " << topic << (rt?" success":" fail"));
	
	return rt;
}

bool ConsumerImpl::Resume(const topic_info_t& topic_info)
{
	bool rt = false;
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		return rt;
	}

	INFO(__FUNCTION__ << " | Resume topic: " << topic_info.topic);
	
	rt = PauseOrResumeConsume(topic_info, true);

	INFO(__FUNCTION__ << " | Resume topic: " << topic_info.topic << (rt?" success":" fail"));
	
	return rt;
}

int32_t GetTopicPartitionCnt(rd_kafka_t *rk, const char* topic)
{
	int cnt = 0;
	
	rd_kafka_topic_t* topic_t = rd_kafka_topic_new(rk, topic, NULL);
	if (NULL != topic_t)
	{
		const rd_kafka_metadata_t *md = NULL;
		rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 0, topic_t, &md, 10*1000);
		if (RD_KAFKA_RESP_ERR_NO_ERROR == err)
		{
			cnt = md->topics->partition_cnt;
		}
		else
		{
			ERROR(__FUNCTION__ << " | fail to getcnt,topic: " << topic << " errmsg: " << rd_kafka_err2str(err));
		}
		if (NULL != md) 
		{
			rd_kafka_metadata_destroy(md);
		}

		rd_kafka_topic_destroy(topic_t);
	}
	else
	{
		ERROR(__FUNCTION__ << " | fail to rd_kafka_topic_new,topic: " << topic << " errmsg: " << rd_kafka_err2str(rd_kafka_last_error()));
	}

	return cnt;
}

std::vector<topic_info_t> GetTopicInfoFromTopicPartitionList(rd_kafka_topic_partition_list_t* parts, const char* topic)
{
	topic_info_t topic_info;
	std::vector<topic_info_t> vTopicInfo;
	for (int i = 0; i < parts->cnt; ++i)
	{
		if (0 == strcmp(topic, parts->elems[i].topic))
		{
			topic_info.topic = topic;
			topic_info.partition = parts->elems[i].partition;
			topic_info.offset = parts->elems[i].offset;

			vTopicInfo.push_back(topic_info);
		}
	}
	return vTopicInfo;
}

bool ConsumerImpl::PauseOrResumeConsume(const topic_info_t& topic_info, bool resume/*0:pause,1:resume*/)
{
	bool rt = false;
	
	KafkaHandlePtr pKafka = kafka_handle_;
	if (nullptr != pKafka && NULL != pKafka->handle_)
	{
		rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
		rd_kafka_topic_partition_list_t* parts = NULL;
		
		//Returns the current partition assignment
		if ((err = rd_kafka_assignment(pKafka->handle_, &parts)))
		{
			ERROR(__FUNCTION__ << " | failed to rd_kafka_assignment: " << rd_kafka_err2str(err));
			return rt;
		}

		//从parts中找到该topic所有的分区信息
		std::vector<topic_info_t> vTopicInfo = GetTopicInfoFromTopicPartitionList(parts, topic_info.topic.c_str());
		if (!vTopicInfo.empty())
		{
			//申请一个list
			rd_kafka_topic_partition_list_t* one_part = rd_kafka_topic_partition_list_new(vTopicInfo.size());
			if (NULL != one_part)
			{
				auto it = vTopicInfo.begin();
				for (; it != vTopicInfo.end(); ++it)
				{
					rd_kafka_topic_partition_t* topic_to_do = rd_kafka_topic_partition_list_add(one_part, (*it).topic.c_str(), (*it).partition);
					if (NULL == topic_to_do)
					{
						ERROR(__FUNCTION__ << " | failed to rd_kafka_topic_partition_list_add: " << rd_kafka_err2str(err));

						rd_kafka_topic_partition_list_destroy(parts);
						rd_kafka_topic_partition_list_destroy(one_part);
						
						return rt;
					}
					topic_to_do->offset = (*it).offset;
				}
			}
			else
			{
				ERROR(__FUNCTION__ << " | failed to rd_kafka_topic_partition_list_new: " << rd_kafka_err2str(err));

				rd_kafka_topic_partition_list_destroy(parts);
			
				return rt;
			}

			if (resume)
			{
				if ((err = rd_kafka_resume_partitions(pKafka->handle_, one_part)))
				{
					ERROR(__FUNCTION__ << " | failed to rd_kafka_resume_partitions: " << rd_kafka_err2str(err));

					rd_kafka_topic_partition_list_destroy(parts);
					rd_kafka_topic_partition_list_destroy(one_part);
					
					return rt;
				}
			}
			else
			{
				if ((err = rd_kafka_pause_partitions(pKafka->handle_, one_part)))
				{
					ERROR(__FUNCTION__ << " | failed to rd_kafka_pause_partitions: " << rd_kafka_err2str(err));

					rd_kafka_topic_partition_list_destroy(parts);
					rd_kafka_topic_partition_list_destroy(one_part);
					
					return rt;
				}
			}

			rt = true;
		}
		else
		{
			ERROR(__FUNCTION__ << " | GetTopicInfoFromTopicPartitionList empty");
			
			rd_kafka_topic_partition_list_destroy(parts);
		}		
	}
	
	return rt;
}

int ConsumerImpl::AddNewBrokers(const char* brokers)
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

bool ConsumerImpl::IsSubscribed(const std::string& topic)
{
	bool rt = false;
	
	std::lock_guard<std::mutex> lock(topics_mutex_);

	rt = (topics_.find(topic) != topics_.end());

	return rt;
}

bool ConsumerImpl::InternalSubscribe(const std::map<std::string, topic_info_t>& topics, std::string& errmsg)
{
	bool rt = false;
	
	//申请一个topic partition list
	rd_kafka_topic_partition_list_t* rd_kafka_topic_list = rd_kafka_topic_partition_list_new(topics.size());
	if (NULL != rd_kafka_topic_list) 
	{
		auto it = topics.begin();
		for (; it != topics.end(); ++it) 
		{
			//将topic加入topic partition list
			rd_kafka_topic_partition_t* res = rd_kafka_topic_partition_list_add(rd_kafka_topic_list, it->second.topic.c_str(), it->second.partition);
			if (NULL == res) 
			{
				ERROR(__FUNCTION__ << " | Failed to rd_kafka_topic_partition_list_add | group:" << group_ << " | topic: " << it->second.topic);

				errmsg = "rd_kafka_topic_partition_list_add fail";
				rt = false;
				break;
			} 
			else 
			{
				//指定offset
				res->offset = it->second.offset;

				INFO(__FUNCTION__ << " | rd_kafka_topic_partition_list_add success"
					<< " | topic: " << it->second.topic
					<< " | partition: " << it->second.partition
					<< " | offset: " << it->second.offset);
			}

			rt = true;
		}

		KafkaHandlePtr pKafka = kafka_handle_;
		if (rt && nullptr != pKafka && NULL != pKafka->handle_) 
		{
			//订阅
			rd_kafka_resp_err_t err = rd_kafka_subscribe(pKafka->handle_, rd_kafka_topic_list);
			if (RD_KAFKA_RESP_ERR_NO_ERROR != err) 
			{
				ERROR(__FUNCTION__ << " | Failed to rd_kafka_subscribe | err msg:" << rd_kafka_err2str(err));

				errmsg = rd_kafka_err2str(err);
				rt = false;				
			} 
			else 
			{
				it = topics.begin();
				for (; it != topics.end(); ++it)
				{
					INFO(__FUNCTION__ << " | rd_kafka_subscribe success"
						<< " | topic: " << it->second.topic
						<< " | partition: " << it->second.partition
						<< " | offset: " << it->second.offset);
				}
				
				rt = true;
			}  		
		}
		else
		{
			ERROR(__FUNCTION__ << " | no efficacious topics");

			errmsg = "no efficacious topics";
			rt = false;
		}
		//释放topic partition list
		rd_kafka_topic_partition_list_destroy(rd_kafka_topic_list);
		rd_kafka_topic_list = NULL;
	} 
	else 
	{
		ERROR(__FUNCTION__ << " | Failed to rd_kafka_topic_partition_list_new");
		errmsg = "rd_kafka_topic_partition_list_new fail";
	}

	return rt;
}

bool ConsumerImpl::Subscribe(const std::vector<topic_info_t>& topics, std::string& errmsg) 
{
	bool rt = false;

	if (!topics.empty()) 
	{			
		auto itIn = topics.begin();
		for (; itIn != topics.end(); ++itIn)
		{
			topics_.insert(std::make_pair((*itIn).topic, (*itIn)));
		}
		
		//订阅失败,清空
		if (!(rt = InternalSubscribe(topics_, errmsg)))
		{
			ERROR(__FUNCTION__ << " | subscribe fali");
		}
	}
	else
	{
		ERROR(__FUNCTION__ << " | topics is empty");

		errmsg = "topics is empty";
	}

	return rt;
}

void ConsumerImpl::Stop(int timeout_ms) 
{
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		return;
	}
	INFO(__FUNCTION__ << " | Startting Stop...");
	
	INFO(__FUNCTION__ << " | Startting consumer clean up...");

	KafkaHandlePtr pKafka = kafka_handle_;
	if (nullptr != pKafka && NULL != pKafka->handle_)
	{
		//TODO: 要立刻置一个标志,让Poll不要再消费,让commitoffset接口返回失败
		
		//把wait_commit_offsets_中的提交
		std::string e; NotifyCommitOffSet(0, e);
		
		rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
		
		//需要强制把rdkafka未提交的全部提交....
		err = rd_kafka_commit(pKafka->handle_, NULL, 0);
		if (RD_KAFKA_RESP_ERR_NO_ERROR != err) 
		{
			ERROR(__FUNCTION__ << " | Failed to close consumer | err msg: " << rd_kafka_err2str(err));
		}

		//关闭消费
		err = rd_kafka_consumer_close(pKafka->handle_);
		if (RD_KAFKA_RESP_ERR_NO_ERROR != err) 
		{
			ERROR(__FUNCTION__ << " | Failed to close consumer | err msg: " << rd_kafka_err2str(err));
		}
	}
	INFO(__FUNCTION__ << " | Consumer clean up done!");
	
	INFO(__FUNCTION__ << " | Finished Stop");
}

void ConsumerImpl::UnInit(int timeout_ms) 
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
		//TODO: 要立刻置一个标志,让Poll不要再消费,让commitoffset接口返回失败

		//把wait_commit_offsets_中的提交
		std::string e; NotifyCommitOffSet(0, e);
		
		rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;

		//需要强制把未提交的全部提交....
		err = rd_kafka_commit(pKafka->handle_, NULL, 0);
		if (RD_KAFKA_RESP_ERR_NO_ERROR != err) 
		{
			ERROR(__FUNCTION__ << " | Failed to close consumer | err msg: " << rd_kafka_err2str(err));
		}
		
		INFO(__FUNCTION__ << " | Startting destory rdkafka...");

		pKafka.reset();

		INFO(__FUNCTION__ << " | rdkafka destory done!");
	}
	
	rd_kafka_wait_destroyed(timeout_ms);

	if (is_init_) is_init_ = false;

	INFO(__FUNCTION__ << " | Finished uninit");
}

bool ConsumerImpl::Poll(int timeout_ms, std::string& errmsg)
{
	bool rt = false;
	
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		usleep(timeout_ms*1000);
		errmsg = "has not inited";
		return rt;
	}

	KafkaHandlePtr pKafka = kafka_handle_;
	if (nullptr != pKafka && NULL != pKafka->handle_)
	{
		//TODO: 如果当前正在退出,要控制不要再poll
		//TODO: 判断是否待提交的offset过多,超过设定值后,强制提交
		
		rd_kafka_message_t* rk_msg = rd_kafka_consumer_poll(pKafka->handle_, timeout_ms);
		if (NULL == rk_msg || (NULL != rk_msg && RD_KAFKA_RESP_ERR_NO_ERROR != rk_msg->err)) 
		{
			rd_kafka_resp_err_t err = rd_kafka_last_error();

			//超时/无可消费的数据
			if (RD_KAFKA_RESP_ERR__TIMED_OUT != err &&  RD_KAFKA_RESP_ERR__PARTITION_EOF != err)
			{
				ERROR(__FUNCTION__ << " | Failed to consumer poll | err: " << err << " | err msg: " << rd_kafka_err2str(err));
				errmsg = rd_kafka_err2str(err);
				rt = false;
			}
			else
			{
				errmsg = rd_kafka_err2str(err);
				rt = true;
			}

			//无数据消费时,通知offset提交(如有待提交)
			std::string e; NotifyCommitOffSet(0, e);
		}
		else
		{
			//only call success msg....
			rdkafka_consume_cb(rk_msg);
			errmsg = "success";
			rt = true;
		}
	}
	else
	{
		usleep(timeout_ms*1000);
		errmsg = "rdkafka handle ptr error";
	}

	return rt;
}

//重写了官方的提交函数,用于实现回传一个opaque指针
rd_kafka_resp_err_t kafka_batch_commit_message(rd_kafka_t *rk, const std::vector<ConsumedMessagePtr>& vMsgForCommit) 
{
	rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR__INVALID_ARG;

	int64_t commit_tm_ms = MWkfkHelper::GetCurrentTimeMs();
	
    rd_kafka_topic_partition_list_t *offsets = rd_kafka_topic_partition_list_new(vMsgForCommit.size());
	if (NULL != offsets)
	{
		auto it = vMsgForCommit.begin();
		for (;it != vMsgForCommit.end(); ++it)
		{
			rd_kafka_topic_partition_t *rktpar = rd_kafka_topic_partition_list_add(offsets, (*it)->topic.c_str(), (*it)->partition);
			if (NULL != rktpar)
			{
	    		rktpar->offset = (*it)->offset+1;

				//给一个私有数据,回调时用
				tMsgPrivateData* pPrivate = new tMsgPrivateData();
				if (NULL != pPrivate)
				{
					pPrivate->any_ = (*it);
					pPrivate->in_tm_ms_ = commit_tm_ms;
					(*it)->commit_tm_ms = commit_tm_ms;
					rktpar->opaque = static_cast<void*>(pPrivate);
				}
			}
		}
		err = rd_kafka_commit(rk, offsets, 0);
    	rd_kafka_topic_partition_list_destroy(offsets);
		offsets = NULL;
	}
	
    return err;
}

bool ConsumerImpl::NotifyCommitOffSet(int async, std::string& errmsg)
{
	bool rt = false;
	
	KafkaHandlePtr pKafka = kafka_handle_;
	if (nullptr != pKafka && NULL != pKafka->handle_)
	{		
		if (!wait_commit_offsets_.empty() //非空
			&& (!async  //要求同步提交或达到提交条件
			      || (wait_commit_offsets_.size() >= max_wait_commit_ 
			          || MWkfkHelper::GetCurrentTimeMs()-last_commit_tm_>=commit_freq_
			         )
			   )
			)
		{			
			last_commit_tm_ = MWkfkHelper::GetCurrentTimeMs();
			
			std::vector<ConsumedMessagePtr> vMsgForCommit;
			
			{
				std::lock_guard<std::mutex> lock(lock_wait_commit_);
				vMsgForCommit = wait_commit_offsets_;
				wait_commit_offsets_.clear();
			}

			if (!vMsgForCommit.empty())
			{
				rd_kafka_resp_err_t err = kafka_batch_commit_message(pKafka->handle_, vMsgForCommit);
				if (RD_KAFKA_RESP_ERR_NO_ERROR != rt) 
				{
					ERROR(__FUNCTION__ << " | Failed to rd_kafka_commit_message | error msg: " << rd_kafka_err2str(err));
					errmsg = rd_kafka_err2str(err);
					rt = false;
				}
				else
				{
					DEBUG(__FUNCTION__ << " | commit success");
					errmsg = rd_kafka_err2str(err);
					rt = true;
				}
			}
			else
			{
				DEBUG(__FUNCTION__ << " | wait commit offsets is empty");
				rt = true;
			}
		}
		else
		{
			DEBUG(__FUNCTION__ << " | have not reached the commit conditions");
			rt = true;
		}
	}
	else
	{
		errmsg = "rdkafka handle ptr error";
	}

	return rt;
}

bool ConsumerImpl::CommitOffset(const ConsumedMessagePtr& msg_for_commit, int async, std::string& errmsg)
{
	bool rt = false;
		
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		errmsg = "has not inited";
		return rt;
	}

	//先缓存起来
	{
		std::lock_guard<std::mutex> lock(lock_wait_commit_);
		wait_commit_offsets_.push_back(msg_for_commit);
	}

	//再根据是同步还是异步,来决定立刻提交,还是等到一数量后再提交
	rt = NotifyCommitOffSet(async, errmsg);

	return rt;
}

bool ConsumerImpl::CommitOffsetBatch(const std::vector<ConsumedMessagePtr>& vMsgForCommit, int async, std::string& errmsg)
{
	bool rt = false;
	
	if (!is_init_)
	{
		ERROR(__FUNCTION__ << " | has not inited");
		errmsg = "has not inited";
		return rt;
	}

	//先缓存起来
	{
		std::lock_guard<std::mutex> lock(lock_wait_commit_);
		wait_commit_offsets_.insert(wait_commit_offsets_.end(), vMsgForCommit.begin(), vMsgForCommit.end());
	}

	//再根据是同步还是异步,来决定立刻提交,还是等到一数量后再提交
	rt = NotifyCommitOffSet(async, errmsg);

	return rt;
}

void ConsumerImpl::rdkafka_rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque) 
{
	DEBUG(__FUNCTION__ << "rebalance begin");

	switch (err) 
	{
		case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
			{
				DEBUG(__FUNCTION__ << " | rebalnace result OK: " << MWkfkHelper::FormatTopicPartitionList(partitions));
				
				rd_kafka_assign(rk, partitions);
			}
			break;
		case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
			{
				DEBUG(__FUNCTION__ << " | rebalnace result revoke | msg: " << rd_kafka_err2str(err) << " | " <<  MWkfkHelper::FormatTopicPartitionList(partitions));

				//!!!一定要先将未提交的全部提交,这里只能用同步提交,否则可能丢数据
				rd_kafka_commit(rk, partitions, 0);

				rd_kafka_assign(rk, NULL);
			}
			break;
		default:
			{
				ERROR(__FUNCTION__ << " | Failed to rebalance | err msg: " << rd_kafka_err2str(err));
				
				rd_kafka_assign(rk, NULL);
			}
			break;
	}

	
	DEBUG(__FUNCTION__ << "rebalance end");
}

void ConsumerImpl::consume_cb(rd_kafka_message_t *rkmessage, void *opaque)
{
	return;
}

void ConsumerImpl::rdkafka_consume_cb(rd_kafka_message_t * rk_msg)
{
	if (NULL == rk_msg)
	{
		ERROR(__FUNCTION__ << " | consume_cb rk_msg is null");
		return;
	}

	int64_t cb_tm_ms = MWkfkHelper::GetCurrentTimeMs();
	
	//托管给智能指针,防止漏析构
	std::shared_ptr<rk_msg_safe_destory> pSafeRkMsg(new rk_msg_safe_destory(rk_msg));
	
	DEBUG(__FUNCTION__ 
		<< " | consume cb | errmsg: " << rd_kafka_err2str(rk_msg->err)
		<< " | topic: " << rd_kafka_topic_name(rk_msg->rkt) 
		<< " | partition: " << rk_msg->partition
		<< " | offset: " << rk_msg->offset
		<< " | msglen: " << rk_msg->len
		<< " | msg: " << static_cast<const char*>(rk_msg->payload)
		<< " | keylen: " << rk_msg->key_len
		<< " | key: " << static_cast<const char*>(rk_msg->key));

	//only no error,then,callback
	if (RD_KAFKA_RESP_ERR_NO_ERROR == rk_msg->err)
	{	
		ConsumedMessagePtr pMsg(new consumed_message_t());
		if (nullptr != pMsg)
		{
			pMsg->topic = rd_kafka_topic_name(rk_msg->rkt);
			pMsg->data.assign(static_cast<const char*>(rk_msg->payload), rk_msg->len);
			pMsg->key.assign(static_cast<const char*>(rk_msg->key), rk_msg->key_len);
			pMsg->partition = rk_msg->partition;
			pMsg->offset = rk_msg->offset;
			pMsg->errcode = rk_msg->err;
			pMsg->errmsg = rd_kafka_err2str(rk_msg->err);
			pMsg->cb_tm_ms = cb_tm_ms;
			pMsg->commit_tm_ms = 0;	
			pMsg->committed_tm_ms = 0;

			//callback message to application
			if (consumed_cb_)
			{
				consumed_cb_(pInvoker_, pMsg);
			}
			else
			{
				WARNING(__FUNCTION__<< " | consumed_cb ptr is null");
			}
		}
		else
		{
			ERROR(__FUNCTION__ << " | ConsumedMessagePtr alloc error");
		}		
	}
	
}

void ConsumerImpl::rdkafka_offset_commit_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *offsets, void *opaque)
{
	ConsumerImpl* pConsumer = static_cast<ConsumerImpl*>(opaque);
	if (NULL == pConsumer) 
	{
		ERROR(__FUNCTION__ << " | Failed to static_cast from opaque to ConsumerImpl");
		return;
	}
	if (NULL == offsets)
	{
		ERROR(__FUNCTION__ << " | rd_kafka_topic_partition_list_t offsets is null");
		return;
	}
	
	//* If no partitions had valid offsets to commit this callback will be called
	//* with \p err == RD_KAFKA_RESP_ERR__NO_OFFSET which is not to be considered
	//* an error.
	if (RD_KAFKA_RESP_ERR__NO_OFFSET == err)
	{
		WARNING(__FUNCTION__ << " | no partitions had valid offsets to commit");
	}

	int64_t committed_tm_ms = MWkfkHelper::GetCurrentTimeMs();
	
	for (int i = 0; i < offsets->cnt; ++i)
	{
		DEBUG(__FUNCTION__ 
			<< " | offset_commit_cb | errmsg: " << rd_kafka_err2str(offsets->elems[i].err)
			<< " | topic: " << offsets->elems[i].topic 
			<< " | partition: " << offsets->elems[i].partition
			<< " | offset: " << offsets->elems[i].offset
			<< " | metadatalen: " << offsets->elems[i].metadata_size
			<< " | metadata: " << static_cast<const char*>(offsets->elems[i].metadata));

		tMsgPrivateData* pPrivate = static_cast<tMsgPrivateData*>(offsets->elems[i].opaque);
		if (NULL != pPrivate)
		{
			//托管给智能指针,防止漏析构
			std::shared_ptr<tMsgPrivateData> sptr_private(pPrivate);

			ConsumedMessagePtr pMsg(boost::any_cast<ConsumedMessagePtr>(pPrivate->any_));
			if (nullptr != pMsg)
			{
				//no matter error or not,all need callback
				if (pConsumer->offsetcommitted_cb_)
				{
					pMsg->committed_tm_ms = committed_tm_ms;
					pConsumer->offsetcommitted_cb_(pConsumer->pInvoker_, pMsg);
				}
				else
				{
					WARNING(__FUNCTION__<< " | offsetcommitted_cb_ ptr is null");
				}
			}
			else
			{
				DEBUG(__FUNCTION__<< " | cast private->any_ to ConsumedMessagePtr fail");
			}
		}
		else
		{
			DEBUG(__FUNCTION__<< " | cast offsets->elems[i].opaque to tMsgPrivateData fail");
		}
	}	
}

static __attribute__((destructor)) void end() 
{
	LUtil::Logger::uninit();
}

}//namespace mwkfk
