#ifndef MWKFK_CONSUMER_IMP_H_
#define MWKFK_CONSUMER_IMP_H_

#include <string>
#include <set>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include "include/librdkafka/rdkafka.h"
#include "mwkfk_config.h"
#include "mwkfk_consumer.h"

namespace mwkfk 
{
//rd_kafka_message_t��ȫ�ͷ�(���������ָ��)
struct rk_msg_safe_destory
{
	rd_kafka_message_t* rk_msg;

	rk_msg_safe_destory()
	{
		rk_msg = NULL;
	}
	rk_msg_safe_destory(rd_kafka_message_t* p)
	{
		rk_msg = p;
	}
	~rk_msg_safe_destory()
	{
		if (NULL != rk_msg)
		{
			rd_kafka_message_destroy(rk_msg);	
			rk_msg = NULL;
		}
	}
};

//��RDKAFKA�������Ϣ��˽������
struct tMsgPrivateData
{
	//��������
	boost::any any_;
	//���Դ���//����Ϣ������Ͷ�ݵĴ���(��ϢͶ�ݻص�����Ͷ��ʧ�ܺ�,�ϲ�Ҫ��ײ�����ʱ�ü����Ż�����)	-- �ݲ��ṩ���ϲ�ʹ��
	int retry_cnt_;	
	//����Ϣ����produce�ӿڵ�ʱ��
	int64_t in_tm_ms_;
	tMsgPrivateData()
	{
		retry_cnt_ = 0;
	}
};
	
//rdkafka�����������Ϣ
struct tKafkaHandle
{
	//rdkafka��������
	rd_kafka_t* handle_;
	tKafkaHandle()
	{
		handle_ = NULL;
	}
	~tKafkaHandle()
	{
		//�������
		if (handle_ != NULL)
		{
			rd_kafka_destroy(handle_);
			handle_ = NULL;
		}
	}
};
typedef std::shared_ptr<tKafkaHandle> KafkaHandlePtr;

class ConsumerImpl 
	:public std::enable_shared_from_this<ConsumerImpl>
{
public:
	ConsumerImpl();
	~ConsumerImpl();

public:
	bool Init(const std::string& group, const std::string& broker_list, const std::string& log_path, const std::string& config_path);

	void SetConsumedCallBack(void* pInvoker, pfunc_on_msgconsumed pConsumeCb);
	
	void SetOffsetCommitCallBack(void* pInvoker, pfunc_on_offsetcommitted pOffsetCommitted);

	bool Subscribe(const std::vector<topic_info_t>& topics, std::string& errmsg);

	bool AddTopic(const topic_info_t& topic_info);

	bool Poll(int timeout_ms, std::string& errmsg);

	void Stop(int timeout_ms);

	void UnInit(int timeout_ms);
	
public:
	//TODO: �첽�ύʧ�ܵĻ�,һ��Ҫ�ص�
	bool CommitOffset(const ConsumedMessagePtr& msg_for_commit, int async, std::string& errmsg); 

	bool CommitOffsetBatch(const std::vector<ConsumedMessagePtr>& vMsgForCommit, int async, std::string& errmsg); 
	
	bool Pause(const std::string& topic);

	bool Resume(const std::string& topic);

	bool Resume(const topic_info_t& topic_info);
	
public:
 	int AddNewBrokers(const char* brokers);

	void SetOffsetCommitPolicy(size_t max_wait_commit, int commit_freq);
	
private:

	//TODO: �첽�ύʧ�ܵĻ�,һ��Ҫ�ص�
	bool NotifyCommitOffSet(int async, std::string& errmsg);

	static void rdkafka_rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err,rd_kafka_topic_partition_list_t *partitions, void *opaque); 

	void rdkafka_consume_cb(rd_kafka_message_t *rk_msg);
	
	static void consume_cb(rd_kafka_message_t *rkmessage, void *opaque);

	static void rdkafka_offset_commit_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *offsets, void *opaque);
	
	bool InitRdKafkaHandle(rd_kafka_conf_t* rd_kafka_conf);
	
	bool InitRdKafkaConfig(rd_kafka_conf_t** p_srd_kafka_conf);

	bool PauseOrResumeConsume(const topic_info_t& topic_info, bool resume/*0:pause,1:resume*/);

	bool InternalSubscribe(const std::map<std::string, topic_info_t>& topics, std::string& errmsg);

	bool IsSubscribed(const std::string& topic);

private:
	KafkaHandlePtr kafka_handle_;

	std::string broker_list_;

	std::mutex topics_mutex_;
	std::map<std::string, topic_info_t> topics_;

	bool is_init_;

	MWkfkConfigLoader config_loader_;

	std::string group_;

	size_t	max_wait_commit_;
	int  commit_freq_;
	
	//��Ϣ���ѻص�
	pfunc_on_msgconsumed consumed_cb_;
	//��Ϣoffset�ύ�ص�
	pfunc_on_offsetcommitted offsetcommitted_cb_;
	//�ϲ������ָ��
	void* pInvoker_;

	//���ύ����
	std::mutex lock_wait_commit_;
	std::vector<ConsumedMessagePtr> wait_commit_offsets_;

	//�ϴ��ύ��ʱ��
	//TODO: Ҫԭ�Ӳ���
	int64_t last_commit_tm_;
};
}//namespace mwkfk

#endif//MWKFK_PRODUCER_IMP_H_
