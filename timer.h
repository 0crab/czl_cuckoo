#pragma once

#include <atomic>
#include <iostream>
#include <random>
#include <chrono>
#include <fstream>
#include <pthread.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include "assert.h"


using namespace std;

class Tracer {
    timeval begTime;

    timeval endTime;

    long duration;

public:
    void startTime() {
        gettimeofday(&begTime, nullptr);
    }

    long getRunTime() {
        gettimeofday(&endTime, nullptr);
        //cout << endTime.tv_sec << "<->" << endTime.tv_usec << "\t";
        duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
        begTime = endTime;
        return duration;
    }

    long fetchTime() {
        gettimeofday(&endTime, nullptr);
        duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
        return duration;
    }
};



class Timer{
public:
    Timer(int thread_n){
        thread_num = thread_n;
        tracers =  new Tracer[thread_num];
        runtime_list = new long[thread_num];
        op_count_list = new long[thread_num]();
        record = new bool[thread_num];
        for(int i = 0; i < thread_num; i++) record[i] = false;
    }

    void startTime(int tid) {tracers[tid].startTime();}

    long getRunTime(int tid) { return tracers[tid].getRunTime();}

    long fetchTime(int tid){ return tracers[tid].fetchTime();}

    void add_op_count(int tid,long op_count){
        op_count_list[tid] += op_count;
    }

    void recordRunTime(int tid){
        runtime_list[tid] = getRunTime(tid);
        record[tid] = true;
    }

    double getThroughPut(){
        bool allrecord = true;
        for(int i =0; i < thread_num; i++)
            if(!record[i])
                allrecord = false;
        assert(allrecord);

        double runtime = 0,total_count = 0;
        for(size_t i = 0 ; i < thread_num; i++){
            total_count += op_count_list[i];
            runtime += runtime_list[i];
        }

        runtime /= thread_num;
        return  total_count * 1.0 / runtime;
    }

private:
    int thread_num;
    bool * record;
    Tracer * tracers;
    long * runtime_list;
    long * op_count_list;
};


class ThreadBarrier{
public:
    ThreadBarrier(int thread_n) : thread_num(thread_n){
        pthread_barrier_init(&pbt, nullptr,thread_num);
    }

    void threadWait(){pthread_barrier_wait(&pbt);}

    ~ThreadBarrier(){
        pthread_barrier_destroy(&pbt);
    }

private:
    int thread_num;
    pthread_barrier_t pbt;
};