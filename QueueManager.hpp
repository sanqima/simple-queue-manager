#ifndef QUEUEMANAGER_INCLUDED
#define QUEUEMANAGER_INCLUDED

#include <pthread.h>
#include <deque>

/*!
 * \file QueueManager.hpp
 * \author Arun Chandrasekaran <visionofarun@gmail.com>
 *
 * An interface for using queues.
 */

/*!
 * \class QueueManager
 * \brief A class that manages different types of staged event driven queues
 */
template <class T>
class QueueManager
{
    std::deque<T*>  m_stage1Q;       /**< Deque that will store the stage1 jobs */
    pthread_mutex_t m_stage1QMtx;    /**< Mutex to protect the stage1 deque */
    pthread_cond_t  m_stage1QCnd;    /**< Conditional variable to wait on */

    std::deque<T*>  m_stage2Q;        /**< Address of the root node of the assign queue */
    pthread_mutex_t m_stage2QMtx;     /**< Mutex to protect the stage2 deque */
    pthread_cond_t  m_stage2QCnd;     /**< Conditional variable to wait on */

    static QueueManager<T>* m_jobQueue; /**< Pointer to T Queue instance. */
    static void CleanupQueue(void *arg);

protected:
    QueueManager(size_t max_batch_size);
    virtual ~QueueManager();

public:
    void PushStage1Queue(T* pJob);
    T* PopStage1Queue();

    void PushStage2Queue(T* pJob);
    T* PopStage2Queue();

    /*! @return Returns S1 Length */
    size_t Stage1QueueLength()
    {
        pthread_mutex_lock(&m_stage1QMtx);
        size_t length = m_stage1Q.size();
        pthread_mutex_unlock(&m_stage1QMtx);
        return length;
    }

    /*! @return Returns S2 Length */
    size_t Stage2QueueLen()
    {
        pthread_mutex_lock(&m_stage2QMtx);
        size_t length = m_stage2Q.size();
        pthread_mutex_unlock(&m_stage2QMtx);
        return length;
    }

    ///@name Singleton accesor functions
    //@{
    /*!
     * Gets the QueueManager singleton instance
     * @return Reference to the singleton instance
     */
    static QueueManager<T>& Instance(size_t max_batch_size = 0)
    {
        if (!m_jobQueue)
            m_jobQueue = new QueueManager<T>(max_batch_size);
        return *m_jobQueue;
    }

    /*!
     * Destroys the QueueManager singleton.
     */
    static void Destroy()
    {
        if (m_jobQueue)
        {
            delete m_jobQueue;
            m_jobQueue = NULL;
        }
    }
    //@}
};

///@name Constructor and destructor
//@{
/// Initializes the mutex and condvars and loads the deque with stage1 jobs
template <class T>
inline QueueManager<T>::QueueManager(size_t max_batch_size)
{
    pthread_mutex_init(&m_stage1QMtx, NULL);
    pthread_cond_init(&m_stage1QCnd, NULL);

    pthread_mutex_init(&m_stage2QMtx, NULL);
    pthread_cond_init(&m_stage2QCnd, NULL);

    while (max_batch_size)
    {
        m_stage1Q.push_back(new T());
        max_batch_size--;
    }
}

/// Destroys the mutexes and condvars and cleans up the stage1 job queue.
template <class T>
inline QueueManager<T>::~QueueManager()
{
    pthread_mutex_destroy(&m_stage2QMtx);
    pthread_cond_destroy(&m_stage2QCnd);
    pthread_mutex_destroy(&m_stage1QMtx);
    pthread_cond_destroy(&m_stage1QCnd);

    for (size_t i = 0; i < m_stage1Q.size(); ++i)
        delete m_stage1Q[i];

    m_stage1Q.clear();
}
//@}

template <class T> QueueManager<T>* QueueManager<T>::m_jobQueue = NULL;

/// Clean up handler that helps the thread in exiting safely
template <class T>
inline void QueueManager<T>::CleanupQueue(void *arg)
{
    pthread_mutex_t *pmtx = static_cast<pthread_mutex_t *> (arg);
    pthread_mutex_unlock(pmtx);
}

///@name Stage1 queue accessor functions
//@{
/// Pushes the given element into the stage1 queue
template <class T>
inline void QueueManager<T>::PushStage1Queue(T *pJob)
{
    if (!pJob)
        return;

    pthread_mutex_lock(&m_stage1QMtx);
    m_stage1Q.push_back(pJob);
    pthread_cond_broadcast(&m_stage1QCnd);
    pthread_mutex_unlock(&m_stage1QMtx);
}

/// Pops a job from the stage1 queue. If the queue is empty it will wait on the
/// stage1 condvar
template <class T>
inline T* QueueManager<T>::PopStage1Queue()
{
    T* tmp;
    pthread_mutex_lock(&m_stage1QMtx);
    pthread_cleanup_push(CleanupQueue, &m_stage1QMtx);

    while (m_stage1Q.stage1())
        pthread_cond_wait(&m_stage1QCnd, &m_stage1QMtx);

    tmp = m_stage1Q.front();
    m_stage1Q.pop_front();

    pthread_cleanup_pop(1);
    return tmp;
}
//@}

///@name Stage2 queue accessor functions
//@{
/// Pushes the given element into the stage2 queue
template <class T>
inline void QueueManager<T>::PushStage2Queue(T* pJob)
{
    if (!pJob)
        return;

    pthread_mutex_lock(&m_stage2QMtx);
    m_stage2Q.push_back(pJob);
    pthread_cond_broadcast(&m_stage2QCnd);
    pthread_mutex_unlock(&m_stage2QMtx);
}

/// Pops a job from the stage2 queue. If the queue is empty it will wait on the
/// stage2 condvar
template <class T>
inline T* QueueManager<T>::PopStage2Queue()
{
    T* tmp;
    pthread_mutex_lock(&m_stage2QMtx);
    pthread_cleanup_push(CleanupQueue, &m_stage2QMtx);

    while (!m_stage2Q.size())
        pthread_cond_wait(&m_stage2QCnd, &m_stage2QMtx);

    tmp = m_stage2Q.front();
    m_stage2Q.pop_front();
    pthread_cleanup_pop(1);
    return tmp;
}
//@}
#endif
