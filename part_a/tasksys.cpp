#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    numThreads = num_threads;
    taskCounter = 0;
    
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::parallelSpawnDynamic(IRunnable* runnable, int num_total_tasks) {

    int task_index = taskCounter++;
    while (task_index < num_total_tasks)
    {
        runnable->runTask(task_index, num_total_tasks);
        task_index = taskCounter++;
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    taskCounter = 0;
    std::thread threads[numThreads];
    for (int i = 0; i < numThreads; i++) {
        threads[i] = std::thread(&TaskSystemParallelSpawn::parallelSpawnDynamic, this, runnable, num_total_tasks);
    }

    for (int i = 0; i < numThreads; i++) {
        threads[i].join();
    }

  
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // printf("Started initiatlizer");
    numThreads = num_threads;
    threads = new std::thread[numThreads];
    taskCounter = 0; 
    taskCompleted = 0;
    numTotalTasks = 0;
    globalRunnable = nullptr;
    finished = false;
    for (int i = 0; i < numThreads; i++) {
        threads[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::spinningThreads, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    finished = true;
    globalRunnable = nullptr;
    for (int i = 0; i < numThreads; i++) {
        threads[i].join();
    }
    delete[] threads;
    taskCounter = 0;

}

void TaskSystemParallelThreadPoolSpinning::spinningThreads()
{
    int taskIdx;
    while (!finished) {
        if (globalRunnable != nullptr) {
            if ((taskIdx = taskCounter.fetch_add(1)) < numTotalTasks){
                // printf("taskIdx %d\n", taskIdx);
                globalRunnable->runTask(taskIdx, numTotalTasks);
                if(++taskCompleted == numTotalTasks) {
                    std::unique_lock<std::mutex> lock(mutex);
                    cv.notify_all();
                }
            }
        }
    }
    // printf("Finished\n");
}


void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
	globalRunnable = runnable;
    taskCounter = 0;
    numTotalTasks = num_total_tasks;
    taskCompleted = 0;
    finished = false;
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return taskCompleted == numTotalTasks; });
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    // printf("start init");
    numThreads = num_threads;
    threads = new std::thread[numThreads];
    taskCounter = 0; 
    taskCompleted = 0;
    numTotalTasks = 0;
    globalRunnable = nullptr;
    finished = false;
    cv1 = new std::condition_variable();
    cv2 = new std::condition_variable();
    mutex1_ = new std::mutex();
    mutex2_ = new std::mutex();
    for (int i = 0; i < numThreads; i++) {
        threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::spinningThreads, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    finished = true;
    globalRunnable = nullptr;
    cv1->notify_all();
    for (int i = 0; i < numThreads; i++) {
        threads[i].join();
    }
    delete[] threads;
    taskCounter = 0;
}

void TaskSystemParallelThreadPoolSleeping::spinningThreads() {   
    int taskIdx;
    while (!finished) {
        {
            if (taskCounter > numTotalTasks) {
                std::unique_lock<std::mutex> lock(*mutex1_);
                cv1->wait(lock, [this] { return (taskCounter < numTotalTasks || finished);});
                }
                if (finished) {
                    return;
                }
        }
        if ((taskIdx = taskCounter.fetch_add(1)) < numTotalTasks){
            globalRunnable->runTask(taskIdx, numTotalTasks);
            if(++taskCompleted == numTotalTasks) {
                cv2->notify_one();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

	globalRunnable = runnable;
    taskCounter = 0;
    numTotalTasks = num_total_tasks;
    taskCompleted = 0;
    finished = false;

    cv1->notify_all();
    std::unique_lock<std::mutex> lock(*mutex2_);
    cv2->wait(lock, [this] { return taskCompleted == numTotalTasks; });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
