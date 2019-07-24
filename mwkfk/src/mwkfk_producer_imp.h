#ifndef MWKFK_PRODUCER_IMP_H_
#define MWKFK_PRODUCER_IMP_H_

#include <string>
#include <map>
#include <memory>
#include <mutex>
#include "mwkfk_config.h"

namespace mwkfk 
{
//topic句柄和配置信息
struct tKafkaTopic
{
	//单个topic的配置
    rd_kafka_topic_conf_t* conf_;
	//单个topic句柄
    rd_kafka_topic_t* topic_;
	tKafkaTopic()
	{
		conf_ = NULL;
		topic_ = NULL;
	}
	~tKafkaTopic()
	{
		//conf_不需要析构

		//析构句柄
		if (topic_ != NULL)
		{
			rd_kafka_topic_destroy(topic_);
			topic_ = NULL;
		}
	}
};
typedef std::shared_ptr<tKafkaTopic> KafkaTopicPtr;

//rdkafka句柄和配置信息
struct tKafkaHandle
{
  	//rdkafka对象配置
    rd_kafka_conf_t* conf_;	
	//rdkafka对象名柄
    rd_kafka_t* handle_;
	tKafkaHandle()
	{
		conf_ = NULL;
		handle_ = NULL;
	}
	~tKafkaHandle()
	{
		//conf_不需要析构

		//析构句柄
		if (handle_ != NULL)
		{
			rd_kafka_destroy(handle_);
			handle_ = NULL;
		}
	}
};
typedef std::shared_ptr<tKafkaHandle> KafkaHandlePtr;

//让RDKAFKA保存的消息的私有数据
struct tMsgPrivateData
{
	//任意数据
	boost::any any_;
	//重试次数//该消息被重试投递的次数(消息投递回调返回投递失败后,上层要求底层重试时该计数才会增加)	-- 暂不提供给上层使用
	int retry_cnt_;	
	//该消息调用produce接口的时间
	int64_t in_tm_ms_;
	tMsgPrivateData()
	{
		retry_cnt_ = 0;
	}
};

class ProducerImpl
	:public std::enable_shared_from_this<ProducerImpl>
{
public:

	ProducerImpl();
	~ProducerImpl();
	
public:	
	//初始化时只初始化rdkakfa对象,不要添加topic信息
	bool Init(const std::string& cluster_name, const std::string& log_path, const std::string& config_path);

	//添加需要生产的topic
	bool AddTopic(const std::string& topic_name);

	//反初始化
	void UnInit(int timeout_ms);

	//上层在退出前调用,调用完了以后,等待所有回调返回,然后再调用uninit.
	//该函数会阻塞timeout_ms
	void Stop(int timeout_ms);

	//定时调用,以便通知底层让消息抓紧投递到kafka
	//timeout_ms=0仅代表通知,立刻返回,>0会阻塞
	void Poll(int timeout_ms);

	//生产消息
	bool Produce(const ProduceMessagePtr& msg_ptr, const tMsgPrivateData* pPrivate, std::string& errmsg);

	//2.设置消息投递结果回调函数
	void SetDeliveredCallBack(void* pInvoker, pfunc_on_msgdelivered pOnDeliveredCb);
	
public:
	//增加新的broker
	//如果kafka集群中新增加了结点,可以调用该函数动态增加
	//格式:
	//"broker1 ip:port,broker2_ip:port,..."
 	//"SSL://broker3_ip:port,ssl://broker4_ip:port,..."
 	//返回值:添加成功的broker的个数
 	int AddNewBrokers(const char* brokers);

	//设置最大允许RDKAFKA OUTQUE的大小
	void SetMaxOutQueSize(size_t size);

	//返回RDKAFKA当前待发队列的大小
	size_t GetOutQueSize() const;
	
private:
	//消息投递结果回调
	static void MsgDeliveredCallback(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);

	//分区算法
	static int32_t partitioner_cb(const rd_kafka_topic_t *rkt,
									const void *keydata,
									size_t keylen,
									int32_t partition_cnt,
									void *rkt_opaque,
									void *msg_opaque);

	//TODO: 加载配置文件时要区分是全局还是TOPIC还是SDK
	rd_kafka_conf_t* InitRdKafkaConfig();

	//初始化RDKAFKA句柄
	rd_kafka_t* InitRdKafkaHandle(rd_kafka_conf_t* kafka_conf);

	//TODO: 加载配置文件时要区分是全局还是TOPIC还是SDK
	rd_kafka_topic_conf_t* InitRdKafkaTopicConfig();

	//初始化RDKAFKA TOPIC句柄
	rd_kafka_topic_t* InitRdKafkaTopic(const std::string& topic_name, rd_kafka_t* kafka_handle, rd_kafka_topic_conf_t* topic_conf);

	//消息生产函数
	bool InternalProduce(rd_kafka_topic_t* kafka_topic, const char* data, size_t data_len, const char* key, size_t key_len, void *msg_opaque, std::string& errmsg);

	//根据topic名称获取rdkafak_topic句柄
	KafkaTopicPtr GetTopic(const std::string& topic_name);
	
private:
	//rdkafka句柄
	KafkaHandlePtr kafka_handle_;
	//topic对象存储,key:topic_name,value:rd_kafka_topic
	std::mutex lock_topic;
	std::map<std::string, KafkaTopicPtr> kafka_topics_;
	//broker列表
	std::string broker_list_;
	//配置文件加载器
	MWkfkConfigLoader config_loader_;
	//允许rdkafka内部最大的缓冲队列大小
	int  max_rd_kafka_outq_len_;
	//初始化标志 TODO: atomic.....
	bool is_init_;
	//消息投递回调函数
	pfunc_on_msgdelivered delivered_cb_;
	//上层调用类指针
	void* pInvoker_;
};
}
#endif//#define MWKFK_PRODUCER_IMP_H_
