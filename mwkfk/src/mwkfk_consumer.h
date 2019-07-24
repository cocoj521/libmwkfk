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

//topic的信息结构体
//名称,分区,偏移量
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

//消费到的消息结构
//注意:data,key 只能按长度拷贝
//如有需要自行按长度拷贝后再做后续的处理
struct consumed_message_t
{
	std::string topic;		//消息topic_name
	std::string data;		//消息的内容,按size()长度取值
	std::string key;		//消息的key,按size()长度取值
	int32_t partition;		//消息所在分区
	int64_t offset;			//消息所在分区上的偏移量
	int errcode;			//错误码 0:成功       非0:失败,原因见errmsg
	std::string errmsg;		//错误码描述
	
	int64_t cb_tm_ms;		//消费到该消息的时间
	int64_t commit_tm_ms;	//该消息offset提交的时间
	int64_t committed_tm_ms;//收到该消息offset成功提交回执的时间
};

typedef std::shared_ptr<consumed_message_t> ConsumedMessagePtr;

//消息消费成功回调函数
//pInvoker:上层调用的类指针
//cb_msg_ptr:消费到的消息的指针
typedef void (*pfunc_on_msgconsumed)(void* pInvoker, const ConsumedMessagePtr& cb_msg_ptr);

//消息offset提交成功回调函数,会将消费到的消息指针原样返回
//pInvoker:上层调用的类指针
//cb_msg_ptr:offset提交成功的消息(与pfunc_on_msgconsumed中的cb_msg_ptr相同)
typedef void (*pfunc_on_offsetcommitted)(void* pInvoker, const ConsumedMessagePtr& cb_msg_ptr);


class ConsumerImpl;
typedef std::shared_ptr<ConsumerImpl> ConsumerImplPtr;
class Consumer 
{
public:
	Consumer();
	~Consumer();
	
public:
	//1.初始化消费者 
	//group:不能为空,必须指定一个分组ID
	//broker_list:kafka集群中broker的IP和端口(如:192.169.1.123:9092)
	//log_path:日志输出文件地址(含文件名,如:./consumer.log)
	//config_path:kafka配置文件地址(如:./consumer.config)
	bool Init(const std::string& group, const std::string& broker_list, const std::string& log_path, const std::string& config_path);

	//2.设置消息消费结果回调函数
	//pInvoker:上层调用的类指针
	//pConsumeCb:消息消费回调函数
	void SetConsumedCallBack(void* pInvoker, pfunc_on_msgconsumed pConsumeCb);

	//2.设置消息offset提交结果回调函数
	//pInvoker:上层调用的类指针
	//pConsumeCb:消息消费回调函数
	void SetOffsetCommitCallBack(void* pInvoker, pfunc_on_offsetcommitted pOffsetCommitted);

	//3.订阅某一个或多个topic的消息
	//可指定从某一个分区或某一个offset开始消费(建议使用默认值)
	//不可重复调用
	//TODO: 增加errmsg字段
	bool Subscribe(const std::vector<topic_info_t>& topics, std::string& errmsg);

	//4.定时调用,以便通知底层让快速从kafka取消息
	//timeout_ms<=0仅代表通知,立刻返回,>0会阻塞,建议值:10
	//返回false时,errmsg表示失败原因
	//建议使用libmwnet库中MWThread创建线程调用
	bool Poll(int timeout_ms, std::string& errmsg);

	//5.提交偏移量,告诉kafka该条消息已被成功消费(注意:默认是必须手动提交!!!)
	//async,0:sync commit, 1:async commit
	//提交后的结果,将通过pfunc_on_offsetcommitted返回,如未设置该回调函数,可不用处理提交结果
	bool CommitOffset(const ConsumedMessagePtr& msg_for_commit, int async, std::string& errmsg);
	bool CommitOffsetBatch(const std::vector<ConsumedMessagePtr>& vMsgForCommit, int async, std::string& errmsg);

	//5.停止消费(停止消费前,要停止一切kafka接口调用,尤其是消费接口和commitoffset接口)
	//上层在退出前必须调用,以便让底层快速强制将缓冲中的信息快速发到KAFKA,调用完了以后,等待所有回调返回,然后再调用UnInit
	//timeout_ms>0该函数会阻塞,超时后则返回,不保证在阻塞期间一定将信息全部送达KAFKA,
	//timeout_ms<=0表示一定要等到信息全部安全到达KAFKA后才返回,但如果KAFKA故障,可能会导致一直无法返回
	void Stop(int timeout_ms=60*1000);

	//6.反初始化消费者.反初始化前最好先调用stop
	//timeout_ms>0该函数会阻塞,超时后则返回,不保证在阻塞期间一定将信息全部送达KAFKA,
	//timeout_ms<=0表示一定要等到信息全部安全到达KAFKA后才返回,但如果KAFKA故障,可能会导致一直无法返回
	void UnInit(int timeout_ms=60*10000);
	
public:

	//动态添加需要消费的topic
	//该函数会先取消当前所有订阅,然后重新订阅,若重新订阅失败,之前订阅的也将无法消费
	//可指定从某一个分区或某一个offset开始消费(建议使用默认值)
	bool AddTopic(const topic_info_t& topic_info);

	//暂停某一个topic的消费
	bool Pause(const std::string& topic);

	//恢复某一个topic的消费
	bool Resume(const std::string& topic);
	
	//恢复某一个topic的消费--暂未实现
	//可以指定从某一个partition,offset开始恢复消费
	bool Resume(const topic_info_t& topic_info);
	
	//TODO: 增加暂停消费一段时间的函数
	//TODO: 增加多长时间后恢复消费的函数
public:
	//增加新的broker
	//如果kafka集群中新增加了结点,可以调用该函数动态增加
	//格式:
	//"broker1 ip:port,broker2_ip:port,..."
 	//"SSL://broker3_ip:port,ssl://broker4_ip:port,..."
 	//返回值:添加成功的broker的个数
 	int AddNewBrokers(const char* brokers);

	//设置offset提交策略--未实现
	//max_wait_commit:最大允许待提交的数量,<=0时表示同步提交
	//commit_freq:批量提交频率,单位:ms,<=0时表示同步提交
	//当待提交数超过max_wait_commit或已经commit_freq长时间未提交了,就会触发批量提交
	void SetOffsetCommitPolicy(size_t max_wait_commit, int commit_freq);
private:
	ConsumerImplPtr consumer_impl_;
};
} //namespace mwkfk
#endif //#ifndef MWKFK_MWKFK_CONSUMER_H_
