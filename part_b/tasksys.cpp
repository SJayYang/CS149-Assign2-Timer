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
    notReadyMutex = new std::mutex();
    readyQueueMutex = new std::mutex();
    syncMutex = new std::mutex();
    readyQueueCv = new std::condition_variable();
    syncCv = new std::condition_variable();
    syncNow = false;

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
        std::unique_lock<std::mutex> readyQueueLock(*readyQueueMutex);
        if (readyQueue.empty()) {
            if (syncNow && notReady.empty() && tasksCompleted == taskIDCounter) {
                syncCv->notify_all();
            }
            readyQueueCv->wait(readyQueueLock, [this] { return !readyQueue.empty() || finishAll; });
        }

        if (finishAll) {
            return;
        }

        struct SubTask current = readyQueue.front();
        readyQueue.pop();
        readyQueueLock.unlock();
        struct BulkTask* curBulkTask = bulkTasks[current.taskID];
        // printf("On Task %d, subtask %d, numTotalTasks %d, subTaskCounter %d\n", current.taskID, current.subTaskID, int(curBulkTask->numTotalTasks), int(curBulkTask->subTaskCompleted));
        curBulkTask->taskRunnable->runTask(current.subTaskID, curBulkTask->numTotalTasks);
        // Possible race condition here? 
        curBulkTask->subTaskCompleted++;
        if (curBulkTask->subTaskCompleted == curBulkTask->numTotalTasks) {
            // printf("taskID %d done\n",curBulkTask->taskID);
            tasksCompleted++;
            curBulkTask->taskFinished = true;
            // printf("taskID %d taskFinished set\n",curBulkTask->taskID);
            for (const TaskID& i : curBulkTask->dependsOn) {
                bulkTasks[i]->dependencies--;
                printf("dependencies %d", int(bulkTasks[i]->dependencies));
                if (bulkTasks[i]->dependencies == 0) {
                    notReadyMutex->lock();
                    notReady.erase(i);
                    notReadyMutex->unlock();
                    addSubTasksQueue(i);
                    printf("Adding new item to queue");
                }
            printf("finished done loop\n");
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
        std::unique_lock<std::mutex> readyQueueLock(*readyQueueMutex);
        // printf("Adding TaskID %d, subTaskID %d, numTotalTasks %d\n", newSubTask.taskID, newSubTask.subTaskID, bulkTasks[curTaskID]->numTotalTasks);
        readyQueue.push(newSubTask);
        }
    }
    // printf("queued %d\n", curTaskID);
    readyQueueCv->notify_all();
}
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

    syncNow = false;
    int curTaskID = taskIDCounter;
    struct BulkTask* curTask = new BulkTask; 
    curTask->taskID = curTaskID;

    int numDependenciesTask = deps.size();
	for (const TaskID& i : deps) {
        if (bulkTasks[i]->taskFinished == true) {
            numDependenciesTask--;
        }
        else {
            bulkTasks[i]->dependsOn.push_back(curTaskID);   
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

    taskIDCounter++;
    return curTaskID;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> syncLock(*syncMutex);
    // syncNow = true;
    printf("waiting on syncLock\n");
    // put a wait variable
    syncCv->wait(syncLock, [this] { syncNow = true; printf("notReady.empty() %d and readyQueue.empty() %d\n", notReady.empty(), readyQueue.empty()); return notReady.empty() && readyQueue.empty(); });
    printf("Finished waiting on syncLock\n");
}
