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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    taskIDCounter = 0;
    tasksCompleted = 0;
    numThreads = num_threads;
    threads = new std::thread[numThreads];
    finishAll = false;
    bulkTaskMutex = new std::mutex();
    notReadyMutex = new std::mutex();
    readyQueueMutex = new std::mutex();
    syncMutex = new std::mutex();
    readyQueueCv = new std::condition_variable();
    syncCv = new std::condition_variable();

    for (int i = 0; i < numThreads; i++) {
        threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::runningThreads, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    finishAll = true;
    readyQueueCv->notify_all();
    for (int i = 0; i < numThreads; i++) {
        threads[i].join();
    }
    delete[] threads;
    delete notReadyMutex;
    delete readyQueueMutex; 
    delete syncMutex;
    delete readyQueueCv;
    delete syncCv;
    bulkTasks.clear();
}

void TaskSystemParallelThreadPoolSleeping::runningThreads() {   
    while (!finishAll) {
        SubTask currentTask;
        {
            std::unique_lock<std::mutex> readyQueueLock(*readyQueueMutex);
            readyQueueCv->wait(readyQueueLock, [this] { return !readyQueue.empty() || finishAll; });

            if (finishAll) return;

			if (!readyQueue.empty()) {
                currentTask = readyQueue.front();
                readyQueue.pop();
            }
            else {
                continue;
            }
        }

        BulkTask* curBulkTask;
        {
            std::unique_lock<std::mutex> bulkTasksLock(*bulkTaskMutex);
            curBulkTask = bulkTasks[currentTask.taskID];
        }

        curBulkTask->taskRunnable->runTask(currentTask.subTaskID, curBulkTask->numTotalTasks);

        if (++curBulkTask->subTaskCompleted == curBulkTask->numTotalTasks) {
            curBulkTask->taskFinished = true;
            tasksCompleted++;

            {
                std::unique_lock<std::mutex> syncLock(*syncMutex);
                if (tasksCompleted == taskIDCounter) {
                    syncCv->notify_one();
                }
            }

            std::vector<TaskID> dependenciesToQueue;
            {
                std::unique_lock<std::mutex> bulkTasksLock(*bulkTaskMutex);
                for (const TaskID& dep : curBulkTask->dependsOn) {
                    
                    if (--bulkTasks[dep]->dependencies == 0) {
                        dependenciesToQueue.push_back(dep);
                    }
                }
            }

            for (TaskID dep : dependenciesToQueue) {
                {
                    std::unique_lock<std::mutex> notReadyLock(*notReadyMutex);
                    notReady.erase(dep);  // Remove from `notReady`
                }
                addSubTasksQueue(dep);  // Add to `readyQueue`
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

	runAsyncWithDeps(runnable, num_total_tasks, std::vector<TaskID>());
    sync();
}

void TaskSystemParallelThreadPoolSleeping::addSubTasksQueue(TaskID curTaskID) {
    struct SubTask newSubTask;
    newSubTask.taskID = curTaskID;

    for (int i = 0; i < bulkTasks[curTaskID]->numTotalTasks; i++) {
        newSubTask.subTaskID = i;
        {
            std::unique_lock<std::mutex> subTaskLock(*readyQueueMutex);
            readyQueue.push(newSubTask);
        }
    }
    readyQueueCv->notify_all();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

    int curTaskID = taskIDCounter++;
    struct BulkTask* curTask = new BulkTask;
    curTask->taskID = curTaskID;
    curTask->numTotalTasks = num_total_tasks;
    curTask->taskRunnable = runnable;
    curTask->subTaskCompleted = 0;
    curTask->taskFinished = false;

    int numDependenciesTask = deps.size();
    {
        std::unique_lock<std::mutex> bulkTasksLock(*bulkTaskMutex);
        for (const TaskID& dep : deps) {
            if (bulkTasks.find(dep) != bulkTasks.end() && bulkTasks[dep]->taskFinished) {
                numDependenciesTask--;
            } else if (bulkTasks.find(dep) != bulkTasks.end()) {
                bulkTasks[dep]->dependsOn.push_back(curTaskID);
            }
        }
        curTask->dependencies = numDependenciesTask;
        bulkTasks[curTaskID] = curTask;
    }

    if (numDependenciesTask == 0) {
        addSubTasksQueue(curTaskID);
    } else {
        std::lock_guard<std::mutex> lock(*notReadyMutex);
        notReady.insert(curTaskID); 
    }

    return curTaskID;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> syncLock(*syncMutex);
    syncCv->wait(syncLock, [this] { return tasksCompleted == taskIDCounter;});
}
