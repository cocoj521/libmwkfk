#ifndef MWKFK_PRODUCER_H_
#define MWKFK_PRODUCER_H_

#include <string>
#include <memory>
#include <boost/any.hpp>

namespace mwkfk 
{
class ProducerImpl;
typedef std::shared_ptr<ProducerImpl> ProducerImplPtr;

//生产消息结构体
//key会参与分区选择算法的计算,尽量填写消息惟一标识,如:消息惟一流水号,如填"",分区将随机选择
//any可以填任意数据,回调返回时会带回
//errmsg如果失败的话返回失败原因
struct produce_message_t
{
////////////////////////////////////////////////////////////////
	//调用produce时只需填写以下几个字段
	std::string topic;		//topic
	std::string data;		//消息内容
	std::string key;		//消息KEY
	boost::any any;			//任意数据
////////////////////////////////////////////////////////////////
	//以下字段在消息投递完成后才会返回,produce时不要填写
	int32_t partition;		//该消息对应的分区
	int64_t offset;			//该消息对应的偏移量
	int errcode;			//错误码 0:成功       非0:失败,原因见errmsg
	std::string errmsg;		//错误码描述
	int status;				//该消息对应的投递状态 0:NOT_PERSISTED 1:POSSIBLY_PERSISTED 2:PERSISTED
							//errcode=0&&status=2时表示成功,其他状态表示失败	
	int64_t in_tm_ms;		//该消息调用produce接口的时间
	int64_t cb_tm_ms;		//该消息回调返回的时间					
};

typedef std::shared_ptr<produce_message_t> ProduceMessagePtr;

//消息投递回调函数
//pInvoker:上层调用的类指针
//cb_msg_ptr:被投递的消息的信息,与produce接口的msg_ptr相同
typedef void (*pfunc_on_msgdelivered)(void* pInvoker, const ProduceMessagePtr& cb_msg_ptr);

class Producer 
{
public:
	Producer();
	~Producer();

public:
	//1.初始化生产者
	//broker_list:kafka集群中broker的IP和端口(如:192.169.1.123:9092)
	//log_path:日志输出文件地址(含文件名,如:./producer.log)
	//config_path:kafka配置文件地址(如:./producer.config)
	bool Init(const std::string& broker_list, const std::string& log_path, const std::string& config_path);

	//2.设置消息投递结果回调函数
	//pInvoker:上层调用的类指针
	//pOnDeliveredCb:消息投递回调函数
	void SetDeliveredCallBack(void* pInvoker, pfunc_on_msgdelivered pOnDeliveredCb);
	
	//3.添加需要生产的topic
	//一个生产者可以负责多个topic的生产
	bool AddTopic(const std::string& topic);

	//4.往kafka生产消息
	//暂不支持指定分区生产
	bool Produce(const ProduceMessagePtr& msg_ptr, std::string& errmsg);

	//5.定时调用,以便通知底层让消息抓紧投递到kafka
	//timeout_ms<=0仅代表通知,立刻返回,>0会阻塞
	//建议使用libmwnet库中MWEventLoop中的RunEvery定时调用,或MWThread启动线程定时调用
	void Poll(int timeout_ms=100);

	//6.停止生产
	//上层在退出前必须调用,以便让底层快速强制将缓冲中的信息快速发到KAFKA,调用完了以后,等待所有回调返回,然后再调用UnInit
	//timeout_ms>0该函数会阻塞,超时后则返回,不保证在阻塞期间一定将信息全部送达KAFKA,
	//timeout_ms<=0表示一定要等到信息全部安全到达KAFKA后才返回,但如果KAFKA故障,可能会导致一直无法返回
	void Stop(int timeout_ms=60*1000);

	//7.反初始化生产者.反初始化前最好先调用stop
	//timeout_ms>0该函数会阻塞,超时后则返回,不保证在阻塞期间一定将信息全部送达KAFKA,
	//timeout_ms<=0表示一定要等到信息全部安全到达KAFKA后才返回,但如果KAFKA故障,可能会导致一直无法返回
	void UnInit(int timeout_ms=60*10000);
public:
	//增加新的broker
	//如果kafka集群中新增加了结点,可以调用该函数动态增加
	//格式:
	//"broker1 ip:port,broker2_ip:port,..."
 	//"SSL://broker3_ip:port,ssl://broker4_ip:port,..."
 	//返回值:添加成功的broker的个数
 	int AddNewBrokers(const char* brokers);
	
	//设置最大允许RDKAFKA OUTQUE的大小
	//如不设置,默认值为10000
	void SetMaxOutQueSize(size_t size);

	//返回RDKAFKA当前待发队列的大小
	size_t GetOutQueSize() const;
private:
	ProducerImplPtr producer_impl_;
};
} //namespace mwkfk
#endif //#define MWKFK_PRODUCER_H_

