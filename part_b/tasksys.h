#ifndef _TASKSYS_H
#define _TASKSYS_H
#include <queue>
#include <map>
#include <vector>
#include <thread>
#include <set>
#include <mutex>
#include <cstdio>
#include <atomic>
#include <condition_variable>

#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

struct BulkTask {
    int taskID;
    int numTotalTasks;
    IRunnable* taskRunnable;
    // keep either dependencies on vector, or just number of tasks is fine
    std::atomic<int> subTaskCompleted;
    std::atomic<int> dependencies;
    std::vector<TaskID> dependsOn;
    std::atomic<bool> taskFinished;
};

struct SubTask {
    int subTaskID;
    int taskID;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        void runningThreads();
        void addSubTasksQueue(TaskID i);
    private:
        std::queue<SubTask> readyQueue;
        std::set<TaskID> notReady;
        std::atomic<int> taskIDCounter;
        std::atomic<int> tasksCompleted;
        std::map<TaskID, BulkTask*> bulkTasks;
        int numThreads;
        std::thread* threads;
        std::atomic<bool> finishAll;
        std::mutex* bulkTaskMutex;
        std::mutex* readyQueueMutex;
        std::mutex* notReadyMutex;
        std::mutex* syncMutex;
        std::condition_variable *readyQueueCv;
        std::condition_variable *syncCv;
};

#endif
