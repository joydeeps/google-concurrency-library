#include "../include/thread.h"
#include "../include/mutex.h"
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include "cppunit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "CppUnit/TestRunner.h"
#include "CppUnit/BriefTestProgressListener.h"
#include "CppUnit/TestResultCollector.h"
#include "CppUnit/TestResult.h"
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <tr1/functional>

namespace tr1 = std::tr1;

void WaitForThenSet(mutex& mu, bool* ready, bool* signal) {
    lock_guard<mutex> l(mu);
    while (!*ready) {
       mu.unlock();
       this_thread::sleep_for(chrono::milliseconds(10));
       mu.lock();
    }
       *signal = true;
}
class ThreadTest: public CppUnit::TestCase
{
public:
	ThreadTest();
	ThreadTest(std::string name);
	~ThreadTest();

	void StartsNewThread();
	void JoinSynchronizes();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};

ThreadTest::ThreadTest(): CppUnit::TestCase()
{

}

ThreadTest::ThreadTest(std::string name): CppUnit::TestCase(name)
{
}


ThreadTest::~ThreadTest()
{
}


void ThreadTest::StartsNewThread()
{
	bool ready = false;
	bool signal = false;
	mutex mu;
	thread thr(tr1::bind(WaitForThenSet, tr1::ref(mu), &ready, &signal));
	lock_guard<mutex> l(mu);
  CPPUNIT_ASSERT (signal == false);
	//EXPECT_FALSE(signal);
	ready = true;
	while (!signal) {
		mu.unlock();
		this_thread::sleep_for(chrono::milliseconds(10));
		mu.lock();
	}
	thr.detach();
}

void ThreadTest::JoinSynchronizes()
{
  bool ready = true;
  bool signal = false;
  mutex mu;
  thread thr(tr1::bind(WaitForThenSet, tr1::ref(mu), &ready, &signal));
  thr.join();
  CPPUNIT_ASSERT (signal == true);
  //EXPECT_TRUE(signal);
}

void ThreadTest::setUp()
{
}


void ThreadTest::tearDown()
{
}


CppUnit::Test* ThreadTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ThreadTest");

  pSuite->addTest (new CppUnit::TestCaller<ThreadTest> ("StartsNewThread", &ThreadTest::StartsNewThread));
  pSuite->addTest (new CppUnit::TestCaller<ThreadTest> ("JoinSynchronizes", &ThreadTest::JoinSynchronizes));
//	CppUnit_addTest(pSuite, ThreadTest, StartsNewThread);
//	CppUnit_addTest(pSuite, ThreadTest, JoinSynchronizes);

	return pSuite;
}

//CPPUNIT_TEST_SUITE_REGISTRATION(ThreadTest);

int main(int argc, char *argv[])
{
  CppUnit::TestResult testresult;
  CppUnit::TestResultCollector collectedResults;
  CppUnit::BriefTestProgressListener progress;

  testresult.addListener(&progress);

  CppUnit::TestRunner testrunner;
  testrunner.addTest(ThreadTest::suite());
  testrunner.run(testresult);
}
