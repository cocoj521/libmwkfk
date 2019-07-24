#include "mwkfk_consumer.h"
#include "util/logger.h"
#include "mwkfk_consumer_imp.h"

namespace mwkfk 
{

Consumer::Consumer()
{
	consumer_impl_.reset(new ConsumerImpl());
}

Consumer::~Consumer() 
{
}

bool Consumer::Init(const std::string& group, const std::string& broker_list, const std::string& log_path, const std::string& config_path) 
{
	bool rt = false;

	if (nullptr != consumer_impl_) 
	{
		rt = consumer_impl_->Init(group, broker_list, log_path, config_path);
		if (rt) 
		{
			INFO(__FUNCTION__ << " | Consumer init is OK!");
		} 
		else 
		{
			ERROR(__FUNCTION__ << " | Failed to init");
		}
	}

	return rt;
}

bool Consumer::Poll(int timeout_ms, std::string& errmsg)
{
	bool rt = false;
	
	errmsg = "success";
	if (nullptr != consumer_impl_) 
	{
		rt = consumer_impl_->Poll(timeout_ms, errmsg);
	}
	else
	{
		errmsg = "consumerimpl ptr is null";
	}

	return rt;
}

void Consumer::Stop(int timeout_ms)
{
	if (nullptr != consumer_impl_) 
	{
		consumer_impl_->Stop(timeout_ms);
	}
	else
	{
		usleep(timeout_ms*1000);
	}
}

void Consumer::UnInit(int timeout_ms) 
{
	if (nullptr != consumer_impl_) 
	{
		consumer_impl_->UnInit(timeout_ms);
	}
}

bool Consumer::CommitOffset(const ConsumedMessagePtr& msg_for_commit, int async, std::string& errmsg)
{
	bool rt = false;

	errmsg = "success";	
	if (nullptr != consumer_impl_) 
	{
		rt = consumer_impl_->CommitOffset(msg_for_commit, async, errmsg);
	}
	else
	{
		errmsg = "consumerimpl ptr is null";
	}
	
	return rt;
}

bool Consumer::CommitOffsetBatch(const std::vector<ConsumedMessagePtr>& vMsgForCommit, int async, std::string& errmsg)
{
	bool rt = false;

	errmsg = "success";	
	if (nullptr != consumer_impl_) 
	{
		rt = consumer_impl_->CommitOffsetBatch(vMsgForCommit, async, errmsg);
	}
	else
	{
		errmsg = "consumerimpl ptr is null";
	}
	
	return rt;
}

void Consumer::SetOffsetCommitPolicy(size_t max_wait_commit, int commit_freq)
{
	if (nullptr != consumer_impl_) 
	{
		consumer_impl_->SetOffsetCommitPolicy(max_wait_commit, commit_freq);
	}
}

void Consumer::SetConsumedCallBack(void* pInvoker, pfunc_on_msgconsumed pConsumeCb)
{
	if (nullptr != consumer_impl_) 
	{
		consumer_impl_->SetConsumedCallBack(pInvoker, pConsumeCb);
	}
}

void Consumer::SetOffsetCommitCallBack(void* pInvoker, pfunc_on_offsetcommitted pOffsetCommitted)
{
	if (nullptr != consumer_impl_) 
	{
		consumer_impl_->SetOffsetCommitCallBack(pInvoker, pOffsetCommitted);
	}
}

bool Consumer::Pause(const std::string& topic)
{
	bool rt = false;

	if (nullptr != consumer_impl_) 
	{
		rt = consumer_impl_->Pause(topic);
	}
	
	return rt;
}

bool Consumer::Resume(const std::string& topic)
{
	bool rt = false;

	if (nullptr != consumer_impl_) 
	{
		rt = consumer_impl_->Resume(topic);
	}

	return rt;
}

bool Consumer::Resume(const topic_info_t& topic_info)
{
	bool rt = false;

	if (nullptr != consumer_impl_) 
	{
		rt = consumer_impl_->Resume(topic_info);
	}

	return rt;
}

bool Consumer::AddTopic(const topic_info_t& topic_info)
{
	bool rt = false;

	if (nullptr != consumer_impl_) 
	{
		rt = consumer_impl_->AddTopic(topic_info);
	}

	return rt;
}

bool Consumer::Subscribe(const std::vector<topic_info_t>& topics, std::string& errmsg)
{
	bool rt = false;

	errmsg = "success";
	if (nullptr != consumer_impl_) 
	{
		rt = consumer_impl_->Subscribe(topics, errmsg);
	}
	else
	{
		errmsg = "consumerimpl ptr is null";
	}

	return rt;
}

int Consumer::AddNewBrokers(const char* brokers)
{
	int cnt = 0;
	
	if (nullptr != consumer_impl_)
	{
		cnt = consumer_impl_->AddNewBrokers(brokers);
	}

	return cnt;
}

}//namespace mwkfk 
