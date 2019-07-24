#ifndef MW_EVENT_LOOP_H
#define MW_EVENT_LOOP_H

#include <boost/any.hpp>
#include <functional>
#include <string>

namespace MWEVENTLOOP
{
	// �¼���Ӧ����
	typedef std::function<void()> EVENT_FUNCTION;
	
	// ��ʱ��ID
	typedef boost::any TIMER_ID;
	
	class EventLoop
	{
	public:
		explicit EventLoop(const std::string& loopname="MWEventLoop");
		~EventLoop();
	public:
		// ��ѭ��������һ��
		void RunOnce(const EVENT_FUNCTION& evfunc);

		// ��ĳһ��ʱ������һ��(��δʵ��)
		//void RunAt();

		// ��ʱһ��ʱ�������һ��
		TIMER_ID RunAfter(double delay, const EVENT_FUNCTION& evfunc);

		// ÿ��һ��ʱ������һ��(��ʱ��)
		TIMER_ID RunEvery(double interval, const EVENT_FUNCTION& evfunc);

		// ȡ����ʱ
		void CancelTimer(TIMER_ID timerid);
		
		// �˳��¼�ѭ��
		void QuitLoop();
	private:
		boost::any m_any;
	};
}
#endif  

/*
����ʾ��:

void MyPrint1()
{
	printf("myprint1....\n");
}

void MyPrint2()
{
	printf("myprint2....\n");
}

void MyPrint3()
{
	printf("myprint3....\n");
}

void DealTimeOut(const boost::any& any_params)
{
	printf("deal timeout data....\n");
}

int main()
{
	MWEVENTLOOP::EventLoop evloop;
	//MWEVENTLOOP::EventLoop evloop("myevloop");

	evloop.RunOnce(MyPrint1);

	// RunAfter��������������߳�ȥ��ѯ��ⳬʱ��������������ʱ������Ϊÿһ������ȥ�İ���һ����ʱ��
	// �ڳ�ʱ�����л�Ӧ�Ŀ��Ե���canceltimerȡ��������������ʱ�ģ������ڳ�ʱ�ص��д���ʱ������
	// ֻ��Ҫִ��RunAfter�����ó�ʱʱ�䣬Ȼ��bind��ʱ�ص������Ͳ�������(����������ָ�����)
	boost::any any_params;
	MWEVENTLOOP::TIMER_ID tmr_id = evloop.RunAfter(10.0, std::bind(DealTimeOut, any_params));
	//evloop.CancelTimer(tmr_id);
	
	evloop.RunAfter(2.0, MyPrint2);

	evloop.RunEvery(1.0, MyPrint3);
}
*/

