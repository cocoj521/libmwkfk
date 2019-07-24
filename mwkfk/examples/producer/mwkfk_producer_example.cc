#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/time.h>

#include <string.h>
#include <memory>
#include <boost/any.hpp>
#include "mwkfk_producer.h"
#include "MWEventLoop.h"

//---------------------------------------
const char* kConfigPath = "./producer.config";
const char* kTopicName1 = "jhbtest";
const char* kTopicName2 = "jhb_kfk_test2";
//const char* kBrokerList = "192.169.3.129:9092";//,192.169.6.233:9092,192.169.6.234:9092";
//const char* kBrokerList = "192.169.6.234:9092";
const char* kBrokerList = "192.169.6.234:9092";
const char* kLogPath = "./producer.log";
const char* kMsg = "BEGIN test msg.....A......test msg.....B.....test msg.....END";

using namespace mwkfk;
using namespace MWEVENTLOOP;

int g_test_num = 200*10000;	

//消息投递回调函数
void func_on_msgdeliver(void* pInvoker, const ProduceMessagePtr& cb_msg_ptr)
{
	if ("" == cb_msg_ptr->key) cb_msg_ptr->key = "0";
		
	if (std::stoi(cb_msg_ptr->key) == g_test_num)
	{
		printf("delivery over:%s\n", cb_msg_ptr->key.c_str());
	}

	if (cb_msg_ptr->errcode != 0 || cb_msg_ptr->status != 2) 
	{
		printf("delivery fail:%s | errmsg:%s\n", cb_msg_ptr->key.c_str(), cb_msg_ptr->errmsg.c_str());
	}

	if (cb_msg_ptr->cb_tm_ms - cb_msg_ptr->in_tm_ms >= 5000)
	{
		printf("delivery larger than:%ld ms | key:%s\n", cb_msg_ptr->cb_tm_ms - cb_msg_ptr->in_tm_ms, cb_msg_ptr->key.c_str());
	}
	//cb_msg.data这段内存是上层申请的,并用调用produce时用了passthrough模式,kafka只会拷贝,不会做其他任何处理,所这里要自已释放
	//如果在调用produce时将这段内存托管给了智能指针,并赋值给了any,这里如果想析构的话,不对cb_msg.any做赋值操作,内存自动会释放;
	//如果调用produce时没有托管给智能指针,这里要自已释放
	//delete cb_msg.data;
	
	return ;
}

//定时poll
void onTimerPoll(mwkfk::Producer& produce)
{
	produce.Poll(0);
}

int main() 
{
	char szBrokerlist[1024] = {0};
	char szTopic[1024] = {0};
	std::string strMsg = "";
	int msg_len = 0;
	printf("input brokerlist:\n");
	scanf("%s", szBrokerlist);

	printf("input topic:\n");
	scanf("%s", szTopic);

	printf("input producer msg size:\n");
	scanf("%d", &msg_len);
	strMsg.resize(msg_len);

	printf("input produce msg count:\n");
	scanf("%d", &g_test_num);

	//快速测试配置
	//....................
	const char* pBrokerList = kBrokerList;//szBrokerlist
	const char* pTopic = szTopic;//kTopicName1;//
	//....................
	
	mwkfk::Producer mwkfk_producer;
	if (!mwkfk_producer.Init(pBrokerList, kLogPath, kConfigPath)) 
	{
		printf("Failed to init kfk_producer\n");
		return 0;
	}
	if (!mwkfk_producer.AddTopic(pTopic))
	{
		printf("Failed to AddTopic:%s\n", pTopic);
		return 0;
	}
	/*
	if (!mwkfk_producer.AddTopic(kTopicName2))
	{
		printf("Failed to AddTopic:%s\n", kTopicName2);
		return 0;
	}
	*/

	mwkfk_producer.SetDeliveredCallBack(NULL, func_on_msgdeliver);

	//int add_cnt = mwkfk_producer.AddNewBrokers("192.169.6.234:9092,192.169.6.211:9092");

	//printf("AddNewBrokers:%d\n", add_cnt);
	
	printf("Init kfk_producer ok!\n");

	MWEVENTLOOP::EventLoop ev_loop;
	ev_loop.RunEvery(0.01, std::bind(onTimerPoll, mwkfk_producer));

	time_t tBegin = time(NULL);
	time_t tTmp = tBegin;

	std::string errmsg;
	for (int i = 0; i < g_test_num; ++i)
	{
		ProduceMessagePtr pMsg(new produce_message_t());
		pMsg->topic = pTopic;
		pMsg->data.resize(msg_len);
		pMsg->key = std::to_string(i+1);
		
		if(!mwkfk_producer.Produce(pMsg, errmsg)) 
		{
			printf("kfk_produce fail...%d-%s\n", i, errmsg.c_str());
			sleep(1);
		}
		/*
		if(!mwkfk_producer.Produce(kTopicName2, strMsg.c_str(), strMsg.size(), std::to_string(i), any, errmsg)) 
		{
			printf("kfk_produce fail...%d\n", i);
			sleep(1);
		}
		*/
		static int s_total = 0;
		if (time(NULL)-tTmp>=1)
		{
			printf("have produced:%d-spd:%d\n", i, i-s_total);
			tTmp = time(NULL);
			s_total = i;
		}
		mwkfk_producer.Poll(0);
	}
	time_t tEnd = time(NULL);
	
	if (tEnd-tBegin > 0) printf("produce speed:%lu\n", g_test_num/(tEnd-tBegin));
	
	while (1)
	{
		mwkfk_producer.Poll(1000);
		sleep(1);
	}
	
	return 0;
}
