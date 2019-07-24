#ifndef MWKFK_PRODUCER_IMP_H_
#define MWKFK_PRODUCER_IMP_H_

#include <string>
#include <map>
#include <memory>
#include <mutex>
#include "mwkfk_config.h"

namespace mwkfk 
{
//topic�����������Ϣ
struct tKafkaTopic
{
	//����topic������
    rd_kafka_topic_conf_t* conf_;
	//����topic���
    rd_kafka_topic_t* topic_;
	tKafkaTopic()
	{
		conf_ = NULL;
		topic_ = NULL;
	}
	~tKafkaTopic()
	{
		//conf_����Ҫ����

		//�������
		if (topic_ != NULL)
		{
			rd_kafka_topic_destroy(topic_);
			topic_ = NULL;
		}
	}
};
typedef std::shared_ptr<tKafkaTopic> KafkaTopicPtr;

//rdkafka�����������Ϣ
struct tKafkaHandle
{
  	//rdkafka��������
    rd_kafka_conf_t* conf_;	
	//rdkafka��������
    rd_kafka_t* handle_;
	tKafkaHandle()
	{
		conf_ = NULL;
		handle_ = NULL;
	}
	~tKafkaHandle()
	{
		//conf_����Ҫ����

		//�������
		if (handle_ != NULL)
		{
			rd_kafka_destroy(handle_);
			handle_ = NULL;
		}
	}
};
typedef std::shared_ptr<tKafkaHandle> KafkaHandlePtr;

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

class ProducerImpl
	:public std::enable_shared_from_this<ProducerImpl>
{
public:

	ProducerImpl();
	~ProducerImpl();
	
public:	
	//��ʼ��ʱֻ��ʼ��rdkakfa����,��Ҫ���topic��Ϣ
	bool Init(const std::string& cluster_name, const std::string& log_path, const std::string& config_path);

	//�����Ҫ������topic
	bool AddTopic(const std::string& topic_name);

	//����ʼ��
	void UnInit(int timeout_ms);

	//�ϲ����˳�ǰ����,���������Ժ�,�ȴ����лص�����,Ȼ���ٵ���uninit.
	//�ú���������timeout_ms
	void Stop(int timeout_ms);

	//��ʱ����,�Ա�֪ͨ�ײ�����Ϣץ��Ͷ�ݵ�kafka
	//timeout_ms=0������֪ͨ,���̷���,>0������
	void Poll(int timeout_ms);

	//������Ϣ
	bool Produce(const ProduceMessagePtr& msg_ptr, const tMsgPrivateData* pPrivate, std::string& errmsg);

	//2.������ϢͶ�ݽ���ص�����
	void SetDeliveredCallBack(void* pInvoker, pfunc_on_msgdelivered pOnDeliveredCb);
	
public:
	//�����µ�broker
	//���kafka��Ⱥ���������˽��,���Ե��øú�����̬����
	//��ʽ:
	//"broker1 ip:port,broker2_ip:port,..."
 	//"SSL://broker3_ip:port,ssl://broker4_ip:port,..."
 	//����ֵ:��ӳɹ���broker�ĸ���
 	int AddNewBrokers(const char* brokers);

	//�����������RDKAFKA OUTQUE�Ĵ�С
	void SetMaxOutQueSize(size_t size);

	//����RDKAFKA��ǰ�������еĴ�С
	size_t GetOutQueSize() const;
	
private:
	//��ϢͶ�ݽ���ص�
	static void MsgDeliveredCallback(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);

	//�����㷨
	static int32_t partitioner_cb(const rd_kafka_topic_t *rkt,
									const void *keydata,
									size_t keylen,
									int32_t partition_cnt,
									void *rkt_opaque,
									void *msg_opaque);

	//TODO: ���������ļ�ʱҪ������ȫ�ֻ���TOPIC����SDK
	rd_kafka_conf_t* InitRdKafkaConfig();

	//��ʼ��RDKAFKA���
	rd_kafka_t* InitRdKafkaHandle(rd_kafka_conf_t* kafka_conf);

	//TODO: ���������ļ�ʱҪ������ȫ�ֻ���TOPIC����SDK
	rd_kafka_topic_conf_t* InitRdKafkaTopicConfig();

	//��ʼ��RDKAFKA TOPIC���
	rd_kafka_topic_t* InitRdKafkaTopic(const std::string& topic_name, rd_kafka_t* kafka_handle, rd_kafka_topic_conf_t* topic_conf);

	//��Ϣ��������
	bool InternalProduce(rd_kafka_topic_t* kafka_topic, const char* data, size_t data_len, const char* key, size_t key_len, void *msg_opaque, std::string& errmsg);

	//����topic���ƻ�ȡrdkafak_topic���
	KafkaTopicPtr GetTopic(const std::string& topic_name);
	
private:
	//rdkafka���
	KafkaHandlePtr kafka_handle_;
	//topic����洢,key:topic_name,value:rd_kafka_topic
	std::mutex lock_topic;
	std::map<std::string, KafkaTopicPtr> kafka_topics_;
	//broker�б�
	std::string broker_list_;
	//�����ļ�������
	MWkfkConfigLoader config_loader_;
	//����rdkafka�ڲ����Ļ�����д�С
	int  max_rd_kafka_outq_len_;
	//��ʼ����־ TODO: atomic.....
	bool is_init_;
	//��ϢͶ�ݻص�����
	pfunc_on_msgdelivered delivered_cb_;
	//�ϲ������ָ��
	void* pInvoker_;
};
}
#endif//#define MWKFK_PRODUCER_IMP_H_
