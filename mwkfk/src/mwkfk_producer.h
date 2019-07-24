#ifndef MWKFK_PRODUCER_H_
#define MWKFK_PRODUCER_H_

#include <string>
#include <memory>
#include <boost/any.hpp>

namespace mwkfk 
{
class ProducerImpl;
typedef std::shared_ptr<ProducerImpl> ProducerImplPtr;

//������Ϣ�ṹ��
//key��������ѡ���㷨�ļ���,������д��ϢΩһ��ʶ,��:��ϢΩһ��ˮ��,����"",���������ѡ��
//any��������������,�ص�����ʱ�����
//errmsg���ʧ�ܵĻ�����ʧ��ԭ��
struct produce_message_t
{
////////////////////////////////////////////////////////////////
	//����produceʱֻ����д���¼����ֶ�
	std::string topic;		//topic
	std::string data;		//��Ϣ����
	std::string key;		//��ϢKEY
	boost::any any;			//��������
////////////////////////////////////////////////////////////////
	//�����ֶ�����ϢͶ����ɺ�Ż᷵��,produceʱ��Ҫ��д
	int32_t partition;		//����Ϣ��Ӧ�ķ���
	int64_t offset;			//����Ϣ��Ӧ��ƫ����
	int errcode;			//������ 0:�ɹ�       ��0:ʧ��,ԭ���errmsg
	std::string errmsg;		//����������
	int status;				//����Ϣ��Ӧ��Ͷ��״̬ 0:NOT_PERSISTED 1:POSSIBLY_PERSISTED 2:PERSISTED
							//errcode=0&&status=2ʱ��ʾ�ɹ�,����״̬��ʾʧ��	
	int64_t in_tm_ms;		//����Ϣ����produce�ӿڵ�ʱ��
	int64_t cb_tm_ms;		//����Ϣ�ص����ص�ʱ��					
};

typedef std::shared_ptr<produce_message_t> ProduceMessagePtr;

//��ϢͶ�ݻص�����
//pInvoker:�ϲ���õ���ָ��
//cb_msg_ptr:��Ͷ�ݵ���Ϣ����Ϣ,��produce�ӿڵ�msg_ptr��ͬ
typedef void (*pfunc_on_msgdelivered)(void* pInvoker, const ProduceMessagePtr& cb_msg_ptr);

class Producer 
{
public:
	Producer();
	~Producer();

public:
	//1.��ʼ��������
	//broker_list:kafka��Ⱥ��broker��IP�Ͷ˿�(��:192.169.1.123:9092)
	//log_path:��־����ļ���ַ(���ļ���,��:./producer.log)
	//config_path:kafka�����ļ���ַ(��:./producer.config)
	bool Init(const std::string& broker_list, const std::string& log_path, const std::string& config_path);

	//2.������ϢͶ�ݽ���ص�����
	//pInvoker:�ϲ���õ���ָ��
	//pOnDeliveredCb:��ϢͶ�ݻص�����
	void SetDeliveredCallBack(void* pInvoker, pfunc_on_msgdelivered pOnDeliveredCb);
	
	//3.�����Ҫ������topic
	//һ�������߿��Ը�����topic������
	bool AddTopic(const std::string& topic);

	//4.��kafka������Ϣ
	//�ݲ�֧��ָ����������
	bool Produce(const ProduceMessagePtr& msg_ptr, std::string& errmsg);

	//5.��ʱ����,�Ա�֪ͨ�ײ�����Ϣץ��Ͷ�ݵ�kafka
	//timeout_ms<=0������֪ͨ,���̷���,>0������
	//����ʹ��libmwnet����MWEventLoop�е�RunEvery��ʱ����,��MWThread�����̶߳�ʱ����
	void Poll(int timeout_ms=100);

	//6.ֹͣ����
	//�ϲ����˳�ǰ�������,�Ա��õײ����ǿ�ƽ������е���Ϣ���ٷ���KAFKA,���������Ժ�,�ȴ����лص�����,Ȼ���ٵ���UnInit
	//timeout_ms>0�ú���������,��ʱ���򷵻�,����֤�������ڼ�һ������Ϣȫ���ʹ�KAFKA,
	//timeout_ms<=0��ʾһ��Ҫ�ȵ���Ϣȫ����ȫ����KAFKA��ŷ���,�����KAFKA����,���ܻᵼ��һֱ�޷�����
	void Stop(int timeout_ms=60*1000);

	//7.����ʼ��������.����ʼ��ǰ����ȵ���stop
	//timeout_ms>0�ú���������,��ʱ���򷵻�,����֤�������ڼ�һ������Ϣȫ���ʹ�KAFKA,
	//timeout_ms<=0��ʾһ��Ҫ�ȵ���Ϣȫ����ȫ����KAFKA��ŷ���,�����KAFKA����,���ܻᵼ��һֱ�޷�����
	void UnInit(int timeout_ms=60*10000);
public:
	//�����µ�broker
	//���kafka��Ⱥ���������˽��,���Ե��øú�����̬����
	//��ʽ:
	//"broker1 ip:port,broker2_ip:port,..."
 	//"SSL://broker3_ip:port,ssl://broker4_ip:port,..."
 	//����ֵ:��ӳɹ���broker�ĸ���
 	int AddNewBrokers(const char* brokers);
	
	//�����������RDKAFKA OUTQUE�Ĵ�С
	//�粻����,Ĭ��ֵΪ10000
	void SetMaxOutQueSize(size_t size);

	//����RDKAFKA��ǰ�������еĴ�С
	size_t GetOutQueSize() const;
private:
	ProducerImplPtr producer_impl_;
};
} //namespace mwkfk
#endif //#define MWKFK_PRODUCER_H_

