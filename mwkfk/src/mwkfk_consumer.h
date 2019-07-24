#ifndef MWKFK_MWKFK_CONSUMER_H_
#define MWKFK_MWKFK_CONSUMER_H_

#include <string>
#include <vector>
#include <memory>

namespace mwkfk 
{
#define OFFSET_BEGINNING -2  /*< Start consuming from beginning of  kafka partition queue: oldest msg */
#define OFFSET_END       -1  /*< Start consuming from end of kafka partition queue: next msg */
#define OFFSET_STORED -1000  /*< Start consuming from offset retrieved from offset store */
#define OFFSET_INVALID -1001 /**< Invalid offset */

//topic����Ϣ�ṹ��
//����,����,ƫ����
struct topic_info_t
{
	std::string topic;
	int32_t partition;
	int64_t offset;

	topic_info_t(const std::string& topic_name)
	{
		topic = topic_name;
		partition = -1;
		offset = OFFSET_INVALID;
	}
	
	topic_info_t()
	{
		topic = "";
		partition = -1;
		offset = OFFSET_INVALID;
	}
};

//���ѵ�����Ϣ�ṹ
//ע��:data,key ֻ�ܰ����ȿ���
//������Ҫ���а����ȿ��������������Ĵ���
struct consumed_message_t
{
	std::string topic;		//��Ϣtopic_name
	std::string data;		//��Ϣ������,��size()����ȡֵ
	std::string key;		//��Ϣ��key,��size()����ȡֵ
	int32_t partition;		//��Ϣ���ڷ���
	int64_t offset;			//��Ϣ���ڷ����ϵ�ƫ����
	int errcode;			//������ 0:�ɹ�       ��0:ʧ��,ԭ���errmsg
	std::string errmsg;		//����������
	
	int64_t cb_tm_ms;		//���ѵ�����Ϣ��ʱ��
	int64_t commit_tm_ms;	//����Ϣoffset�ύ��ʱ��
	int64_t committed_tm_ms;//�յ�����Ϣoffset�ɹ��ύ��ִ��ʱ��
};

typedef std::shared_ptr<consumed_message_t> ConsumedMessagePtr;

//��Ϣ���ѳɹ��ص�����
//pInvoker:�ϲ���õ���ָ��
//cb_msg_ptr:���ѵ�����Ϣ��ָ��
typedef void (*pfunc_on_msgconsumed)(void* pInvoker, const ConsumedMessagePtr& cb_msg_ptr);

//��Ϣoffset�ύ�ɹ��ص�����,�Ὣ���ѵ�����Ϣָ��ԭ������
//pInvoker:�ϲ���õ���ָ��
//cb_msg_ptr:offset�ύ�ɹ�����Ϣ(��pfunc_on_msgconsumed�е�cb_msg_ptr��ͬ)
typedef void (*pfunc_on_offsetcommitted)(void* pInvoker, const ConsumedMessagePtr& cb_msg_ptr);


class ConsumerImpl;
typedef std::shared_ptr<ConsumerImpl> ConsumerImplPtr;
class Consumer 
{
public:
	Consumer();
	~Consumer();
	
public:
	//1.��ʼ�������� 
	//group:����Ϊ��,����ָ��һ������ID
	//broker_list:kafka��Ⱥ��broker��IP�Ͷ˿�(��:192.169.1.123:9092)
	//log_path:��־����ļ���ַ(���ļ���,��:./consumer.log)
	//config_path:kafka�����ļ���ַ(��:./consumer.config)
	bool Init(const std::string& group, const std::string& broker_list, const std::string& log_path, const std::string& config_path);

	//2.������Ϣ���ѽ���ص�����
	//pInvoker:�ϲ���õ���ָ��
	//pConsumeCb:��Ϣ���ѻص�����
	void SetConsumedCallBack(void* pInvoker, pfunc_on_msgconsumed pConsumeCb);

	//2.������Ϣoffset�ύ����ص�����
	//pInvoker:�ϲ���õ���ָ��
	//pConsumeCb:��Ϣ���ѻص�����
	void SetOffsetCommitCallBack(void* pInvoker, pfunc_on_offsetcommitted pOffsetCommitted);

	//3.����ĳһ������topic����Ϣ
	//��ָ����ĳһ��������ĳһ��offset��ʼ����(����ʹ��Ĭ��ֵ)
	//�����ظ�����
	//TODO: ����errmsg�ֶ�
	bool Subscribe(const std::vector<topic_info_t>& topics, std::string& errmsg);

	//4.��ʱ����,�Ա�֪ͨ�ײ��ÿ��ٴ�kafkaȡ��Ϣ
	//timeout_ms<=0������֪ͨ,���̷���,>0������,����ֵ:10
	//����falseʱ,errmsg��ʾʧ��ԭ��
	//����ʹ��libmwnet����MWThread�����̵߳���
	bool Poll(int timeout_ms, std::string& errmsg);

	//5.�ύƫ����,����kafka������Ϣ�ѱ��ɹ�����(ע��:Ĭ���Ǳ����ֶ��ύ!!!)
	//async,0:sync commit, 1:async commit
	//�ύ��Ľ��,��ͨ��pfunc_on_offsetcommitted����,��δ���øûص�����,�ɲ��ô����ύ���
	bool CommitOffset(const ConsumedMessagePtr& msg_for_commit, int async, std::string& errmsg);
	bool CommitOffsetBatch(const std::vector<ConsumedMessagePtr>& vMsgForCommit, int async, std::string& errmsg);

	//5.ֹͣ����(ֹͣ����ǰ,Ҫֹͣһ��kafka�ӿڵ���,���������ѽӿں�commitoffset�ӿ�)
	//�ϲ����˳�ǰ�������,�Ա��õײ����ǿ�ƽ������е���Ϣ���ٷ���KAFKA,���������Ժ�,�ȴ����лص�����,Ȼ���ٵ���UnInit
	//timeout_ms>0�ú���������,��ʱ���򷵻�,����֤�������ڼ�һ������Ϣȫ���ʹ�KAFKA,
	//timeout_ms<=0��ʾһ��Ҫ�ȵ���Ϣȫ����ȫ����KAFKA��ŷ���,�����KAFKA����,���ܻᵼ��һֱ�޷�����
	void Stop(int timeout_ms=60*1000);

	//6.����ʼ��������.����ʼ��ǰ����ȵ���stop
	//timeout_ms>0�ú���������,��ʱ���򷵻�,����֤�������ڼ�һ������Ϣȫ���ʹ�KAFKA,
	//timeout_ms<=0��ʾһ��Ҫ�ȵ���Ϣȫ����ȫ����KAFKA��ŷ���,�����KAFKA����,���ܻᵼ��һֱ�޷�����
	void UnInit(int timeout_ms=60*10000);
	
public:

	//��̬�����Ҫ���ѵ�topic
	//�ú�������ȡ����ǰ���ж���,Ȼ�����¶���,�����¶���ʧ��,֮ǰ���ĵ�Ҳ���޷�����
	//��ָ����ĳһ��������ĳһ��offset��ʼ����(����ʹ��Ĭ��ֵ)
	bool AddTopic(const topic_info_t& topic_info);

	//��ͣĳһ��topic������
	bool Pause(const std::string& topic);

	//�ָ�ĳһ��topic������
	bool Resume(const std::string& topic);
	
	//�ָ�ĳһ��topic������--��δʵ��
	//����ָ����ĳһ��partition,offset��ʼ�ָ�����
	bool Resume(const topic_info_t& topic_info);
	
	//TODO: ������ͣ����һ��ʱ��ĺ���
	//TODO: ���Ӷ೤ʱ���ָ����ѵĺ���
public:
	//�����µ�broker
	//���kafka��Ⱥ���������˽��,���Ե��øú�����̬����
	//��ʽ:
	//"broker1 ip:port,broker2_ip:port,..."
 	//"SSL://broker3_ip:port,ssl://broker4_ip:port,..."
 	//����ֵ:��ӳɹ���broker�ĸ���
 	int AddNewBrokers(const char* brokers);

	//����offset�ύ����--δʵ��
	//max_wait_commit:���������ύ������,<=0ʱ��ʾͬ���ύ
	//commit_freq:�����ύƵ��,��λ:ms,<=0ʱ��ʾͬ���ύ
	//�����ύ������max_wait_commit���Ѿ�commit_freq��ʱ��δ�ύ��,�ͻᴥ�������ύ
	void SetOffsetCommitPolicy(size_t max_wait_commit, int commit_freq);
private:
	ConsumerImplPtr consumer_impl_;
};
} //namespace mwkfk
#endif //#ifndef MWKFK_MWKFK_CONSUMER_H_
