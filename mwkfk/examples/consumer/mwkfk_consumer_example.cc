#include <unistd.h>
#include <signal.h>
#include <atomic>
#include <vector>
#include <list>
#include <string>
#include <iostream>
#include <cstdlib>
#include "mwkfk_consumer.h"
#include "MWThread.h"
#include "MWThreadPool.h"

//----------------------------------------------------
const char* kGroupName = "jhbconsumer";
const char* kTopicName = "jhbtest";
const char* kConfigPath= "./consumer.config";
//const char* kBrokerList = "192.169.6.211:9092";//,192.169.6.233:9092,192.169.6.234:9092";
const char* kBrokerList = "192.169.6.234:9092";

const char* kLogPath = "./consumer.log";


using namespace mwkfk;
using namespace MWTHREAD;
using namespace MWTHREADPOOL;

MWTHREADPOOL::ThreadPool g_thrpool("commit_thr");
mwkfk::Consumer mwkfk_consumer;

void thrPoll(mwkfk::Consumer& consumer)
{
	while (1) 
	{
		std::string errmsg = "";
		bool rt = consumer.Poll(50, errmsg);
		if (!rt)
		{
			printf("consumer pool err:%s\n", errmsg.c_str());
		}
	}

	consumer.Stop(5*1000);
	consumer.UnInit(5*1000);
	
	printf("thrPoll exit\n");
}

std::vector<ConsumedMessagePtr> g_vMsgForCommit;
void commit(mwkfk::Consumer& consumer, const ConsumedMessagePtr& cb_msg_ptr)
{
	std::string errmsg = "";
	if (!consumer.CommitOffset(cb_msg_ptr, 0, errmsg))
	{
		printf("commit fail:%s\n", errmsg.c_str());
	}
}

std::atomic<int> g_total_consumed(0);
//消息消费成功回调函数
//pInvoker:上层调用的类指针
//cb_msg_ptr:消费到的消息的指针
void func_on_msgconsumed(void* pInvoker, const ConsumedMessagePtr& cb_msg_ptr)
{
	/*
	static int total_consumed = 0;
	static time_t tLastConsume = time(NULL);
	++total_consumed;
	if (time(NULL) - tLastConsume > 1)
	{
		printf("total consumed:%d\n", total_consumed);
		time(&tLastConsume);
	}
	*/
	++g_total_consumed;

	//if (8 == g_total_consumed.load())
		//{
	//可以扔到另外一个线程池中去处理消费到的数据,处理完后再commitoffset
	std::string errmsg = "";
	if (!mwkfk_consumer.CommitOffset(cb_msg_ptr, 1, errmsg))
	{
		printf("commit fail:%s\n", errmsg.c_str());
	}
		//}
	//g_thrpool.RunTask(std::bind(commit, mwkfk_consumer, cb_msg_ptr));

	/*
	g_vMsgForCommit.push_back(cb_msg_ptr);
	if (g_vMsgForCommit.size() >= 10000)
	{
		std::string errmsg = "";
		if (!mwkfk_consumer.CommitOffsetBatch(g_vMsgForCommit, 0, errmsg))
		{
			printf("commit fail:%s\n", errmsg.c_str());
		}
		else
		{
			g_vMsgForCommit.clear();
		}
	}
	*/
}

std::atomic<int> g_total_committed(0);

//消息offset提交成功回调函数,会将消费到的消息指针原样返回
//pInvoker:上层调用的类指针
//cb_msg_ptr:offset提交成功的消息(与pfunc_on_msgconsumed中的cb_msg_ptr相同)
void func_on_offsetcommitted(void* pInvoker, const ConsumedMessagePtr& cb_msg_ptr)
{
	/*
	static int total_committed = 0;
	static time_t tLastCommit = time(NULL);
	++total_committed;
	if (time(NULL) - tLastCommit > 1)
	{
		printf("total committed:%d\n", total_committed);
		time(&tLastCommit);
	}
	*/
	++g_total_committed;
}

void* simpleConsumer() 
{
	if (mwkfk_consumer.Init(kGroupName, kBrokerList, kLogPath, kConfigPath)) 
	{
		topic_info_t topic_info;
		topic_info.topic = kTopicName;
		topic_info.partition = -1;
		topic_info.offset = OFFSET_INVALID;
		std::vector<topic_info_t> topics;
		topics.push_back(topic_info);
		std::cout << " topic: " << kTopicName << " | group: " << kGroupName << std::endl;

		mwkfk_consumer.SetConsumedCallBack(NULL, func_on_msgconsumed);
		mwkfk_consumer.SetOffsetCommitCallBack(NULL, func_on_offsetcommitted);

		mwkfk_consumer.SetOffsetCommitPolicy(10000, 1000);
		
		g_thrpool.StartThreadPool(1);

		MWTHREAD::Thread thr1(std::bind(thrPoll, mwkfk_consumer), "thrPoll-1");	
		thr1.StartThread();

		//MWTHREAD::Thread thr2(std::bind(thrPoll, mwkfk_consumer), "thrPoll-2");	
		//thr2.StartThread();

		printf("init ok...\n");

		std::string strErrMsg = "";
		if (mwkfk_consumer.Subscribe(topics, strErrMsg))
		{
			printf("subscribe ok...\n");
			
			int total_consumed_last = 0;
			int total_consumed_this = 0;
			/*
			sleep(5);
			topic_info.topic = "jhbtest2";
			topic_info.partition = -1;
			topic_info.offset = OFFSET_INVALID;
			mwkfk_consumer.AddTopic(topic_info);
			*/
			
			while (1)
			{
				sleep(1);

				total_consumed_this = g_total_consumed.load();
				
				printf("total consumed:%d spd:%d\n", total_consumed_this, total_consumed_this - total_consumed_last);
				printf("total committed:%d\n", g_total_committed.load());

				total_consumed_last = total_consumed_this;

				/*
				static int pausecnt = 0;
				if (++pausecnt > 5)
				{
					printf("pause...\n");
					mwkfk_consumer.Pause(kTopicName);
					pausecnt = 0;
					sleep(5);
					mwkfk_consumer.Resume(kTopicName);
					printf("resume...\n");
				}
				*/
			}
			/*
			while (1) 
			{
				std::string errmsg = "";
				bool rt = mwkfk_consumer.Poll(50, errmsg);
				if (!rt)
				{
					printf("consumer pool err:%s\n", errmsg.c_str());
				}
			}

			mwkfk_consumer.Stop(5*1000);
			mwkfk_consumer.UnInit(5*1000);
			*/	
		} 
		else 
		{
			std::cout << "Failed subscribe" << std::endl;
		}
	} 
	else 
	{
		std::cout << "Failed init" << std::endl;
	}

	return NULL;
}

int main() 
{
    simpleConsumer();

    return 0;
}
