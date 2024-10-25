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
    readyQueueMutex = new std::mutex();
    readyQueueCvMutex = new std::mutex();
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
}

void TaskSystemParallelThreadPoolSleeping::runningThreads() {   
    while(!finishAll){
        {
            if (readyQueue.empty()) {
                if (tasksCompleted > 0 && taskIDCounter == tasksCompleted) {
                    printf("Finished all tasks, notifying...\n");
                    syncCv->notify_all();
                }
                std::unique_lock<std::mutex> readyQueueLock(*readyQueueMutex);
                readyQueueCv->wait(readyQueueLock, [this] { return !readyQueue.empty() || finishAll; });
                printf("readyQueueCv woken up...\n");
            }
            if (finishAll) {
                printf("finishedAll, returning ...\n");
                return;
            }
        }
        readyQueueMutex->lock();
        struct SubTask current = readyQueue.front();
        printf("On Task %d, subtask %d\n", current.taskID, current.subTaskID);
        readyQueue.pop();
        readyQueueMutex->unlock();
        struct BulkTask* curBulkTask = bulkTasks[current.taskID];
        curBulkTask->taskRunnable->runTask(current.subTaskID, curBulkTask->numTotalTasks);
        curBulkTask->subTaskCompleted++;
        if (curBulkTask->subTaskCompleted == curBulkTask->numTotalTasks) {
            tasksCompleted++;
            curBulkTask->taskFinished = true;
            for (const TaskID& i : curBulkTask->dependsOn) {
                bulkTasks[i]->dependencies--;
                if (bulkTasks[i]->dependencies == 0) {
                    notReady.erase(i);
                    addSubTasksQueue(i);
                }
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
        printf("Adding TaskID %d, subTaskID %d\n", newSubTask.taskID, newSubTask.subTaskID);
        readyQueueMutex->lock();
        readyQueue.push(newSubTask);
        readyQueueMutex->unlock();
    }
    readyQueueCv->notify_all();
}
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

    int curTaskID = taskIDCounter;
    struct BulkTask* curTask = new BulkTask; 
    curTask->taskID = curTaskID;

    int numDependenciesTask = deps.size();
	for (const TaskID& i : deps) {
        if (bulkTasks[i]->taskFinished == true) {
            numDependenciesTask--;
        }
    }

    curTask->dependencies = numDependenciesTask;
    curTask->numTotalTasks = num_total_tasks;
    curTask->taskRunnable = runnable;
    curTask->taskFinished = false;
    curTask->subTaskCompleted = 0;
    bulkTasks[curTaskID] = curTask;

    // printf("taskID, %d\n", bulkTasks[curTaskID]->taskID);
    if (deps.size() == 0) {
        addSubTasksQueue(curTaskID);
        taskIDCounter++;
        return curTaskID;
    }
    else {
        notReady.insert(curTaskID);
    }

    // Add this task to depends on in the bulkTasks
	for (const TaskID& i : deps) {
        bulkTasks[i]->dependsOn.push_back(curTaskID);
    }
    taskIDCounter++;
    return curTaskID;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> syncLock(*syncMutex);
    printf("Waiting for sync lock\n");
    syncCv->wait(syncLock, [this] { return notReady.empty() && readyQueue.empty(); });
    printf("sync lock done\n");
}
