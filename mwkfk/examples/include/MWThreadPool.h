#ifndef MW_THREADPOOL_H
#define MW_THREADPOOL_H

#include <boost/any.hpp>
#include <functional>
#include <string>

namespace MWTHREADPOOL
{
	// �̳߳�ִ�е�������
	typedef std::function<void ()> THREAD_TASK;
	
	class ThreadPool
	{
	public:
		explicit ThreadPool(const std::string& poolname);
		~ThreadPool();
	public:
		// Must be called before StartThreadPool().
		// ��������������ֵ,������г�����ֵʱ,�ٴ���ӻ������ȴ�
		// maxquesize<=0��ʾ�����ƴ�С
		void SetMaxTaskQueueSize(int maxquesize);

		// Must be called before StartThreadPool().
		// �����̳߳�ʼ��ʱִ�еĻص�����.�����ʲô��Ҫ���߳�����ǰҪִ�е�,�������øûص�������ʵ��
  		void SetThreadInitCallback(const THREAD_TASK& thrinitcb);

		// ���̳߳�������һ���������߳�
		// thrnum>=0
		void StartThreadPool(int thrnum);

		// ֹͣ�̳߳�
  		void StopThreadPool();

		// �̳߳�����
  		const char* TheadPoolName();

		// ��ǰ������еĴ�С
  		size_t TaskQueueSize() const;

  		// ִ������.���������г������ֵʱ,�ú���������
  		void RunTask(const THREAD_TASK& task);

  		// ����ִ������.���������г������ֵʱ,�ú�����������,�᷵��false.
  		bool TryRunTask(const THREAD_TASK& task);
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
void MyPrint4(int n)
{
	printf("myprint4....%d\n", n);
}

int main(int argc, char* argv[])
{
	{
		MWTHREADPOOL::ThreadPool thrpool("test");
		
		thrpool.SetMaxTaskQueueSize(100);
		thrpool.SetThreadInitCallback(MyPrint1);
		thrpool.StartThreadPool(2);
		thrpool.TryRunTask(MyPrint2);
		thrpool.StopThreadPool();
	}

	{
		MWTHREADPOOL::ThreadPool thrpool("test");
		
		thrpool.SetMaxTaskQueueSize(100);
		thrpool.SetThreadInitCallback(std::bind(MyPrint4,400));
		thrpool.StartThreadPool(2);
		thrpool.TryRunTask(MyPrint1);
		thrpool.TryRunTask(std::bind(MyPrint4,400));
		thrpool.StopThreadPool();
	}

	//{
		std::unique_ptr<MWTHREADPOOL::ThreadPool> pThrPool(new MWTHREADPOOL::ThreadPool("test"));
	
		pThrPool->SetMaxTaskQueueSize(100);
		pThrPool->SetThreadInitCallback(std::bind(MyPrint4,400));
		pThrPool->StartThreadPool(2);
		pThrPool->TryRunTask(MyPrint3);
		pThrPool->TryRunTask(std::bind(MyPrint4,400));
		printf("%s\n", pThrPool->TheadPoolName());
		pThrPool->StopThreadPool();
	//}
		
	while(1)
	{
		sleep(1);
	}
}
*/
