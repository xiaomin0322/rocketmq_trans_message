package org.apache.rocketmq.store.transaction;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.transaction.jdbc.JDBCTransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author fengjian
 * @version V1.0
 * @title: rocketmq-all
 * @Package org.apache.rocketmq.store.transaction
 * @Description:
 * @date 2017/11/7 上午11:25
 */
public class TransactionRecordFlush2DBService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private final TransactionOffsetConifgService transactionOffsetConifgService;

    private final TransactionStore transactionStore;

    public TransactionRecordFlush2DBService(DefaultMessageStore defaultMessageStore) {
        MessageStoreConfig messageStoreConfig = defaultMessageStore.getMessageStoreConfig();
        /**
         * 事物offset维护类
         */
        this.transactionOffsetConifgService = new TransactionOffsetConifgService(defaultMessageStore);
        /**
         * 事物offset维护类
         */
        TransactionTableDefConfigService transactionTableDefConfigService = new TransactionTableDefConfigService(defaultMessageStore);
        transactionTableDefConfigService.load();
        int retry = 0;
        boolean isCheckTable = false;
        /**
         * 数据库事物存储类
         */
        this.transactionStore = new JDBCTransactionStore(messageStoreConfig);
        /**
         * 初始化
         */
        this.transactionStore.load();
        do {
        	//表的后缀
            int tableSuffix = transactionTableDefConfigService.getTableSuffix(messageStoreConfig);
            this.transactionStore.setTableSuffix(tableSuffix);
            log.info("loadTableStoreConfig tableSuffix={}", tableSuffix);
            //初始化表
        } while (!(isCheckTable = transactionStore.createTableIfNotExists()) && ++retry < 5);

        if (!isCheckTable) {
            throw new RuntimeException("check db info error!");
        } else {
        	//持久化配置
            transactionTableDefConfigService.persist();
        }

        for (int i = 0; i < REQUEST_BUFFER_IN_QUEUE; i++) {
            dispatchRequestBufferQueue.add(new DispatchRequestCollections(new AtomicInteger(0), new ArrayList<>()));
        }
    }

   /**
    * 获取本地事物offset
    * @return
    */
    public AtomicLong queryTransactionOffset() {
        return transactionOffsetConifgService.queryOffset();
    }

    @Override
    public void start() {
        super.start();
        /**
         * 定时将内存中的事物offset向文件里面刷
         */
        transactionOffsetConifgService.start();
    }

    public void load() {
    	//加载配置项
        transactionOffsetConifgService.load();

        
        /**
         * 从数据库获取最大最小的offset ,并设置offset
         */
        long maxOffset = transactionStore.maxPK();
        long minOffset = transactionStore.minPK();
        long transactionOffset = queryTransactionOffset().get();
        //set parpare offset
        if (maxOffset > transactionOffset) {
            queryTransactionOffset().compareAndSet(transactionOffset, maxOffset);
            this.maxTransOffset.set(maxOffset);
        } else {
            this.maxTransOffset.set(transactionOffset);
        }
        //set confirm offset
        if (minOffset < transactionOffset && minOffset > 0) {
            this.minTransOffset.set(minOffset);
        } else {
            this.minTransOffset.set(transactionOffset);
        }

    }

    class DispatchRequestCollections {
        private AtomicInteger latch;
        private List<DispatchRequest> requestlist;

        DispatchRequestCollections(AtomicInteger latch, List<DispatchRequest> requestlist) {
            this.latch = latch;
            this.requestlist = requestlist;
        }
    }

    private static final int REQUEST_BUFFER_IN_QUEUE = 5;

    private final AtomicLong maxTransOffset = new AtomicLong(0);

    private final AtomicLong minTransOffset = new AtomicLong(0);

    private static final int FLOW_CONTROLLER = 20000;

    private static final int CHECK_THREAD_LOOP = 100;

    private volatile int flushCounter = 0;

    private volatile ConcurrentLinkedQueue<DispatchRequestCollections> dispatchRequestBufferQueue = new ConcurrentLinkedQueue<>();

    private volatile Semaphore flowController = new Semaphore(FLOW_CONTROLLER);

    private void putEmptyRequestList() {
        dispatchRequestBufferQueue.add(new DispatchRequestCollections(new AtomicInteger(0), new CopyOnWriteArrayList<>()));
    }

    @Override
    public String getServiceName() {
        return TransactionRecordFlush2DBService.class.getName();
    }

    /**
     * 向队列里面加一条消息
     * @param dispatchRequest
     */
    public void appendPreparedTransaction(DispatchRequest dispatchRequest) {
        try {
            flowController.acquire(1);
            while (true) {
                DispatchRequestCollections requests = dispatchRequestBufferQueue.peek();
                if (requests.latch.getAndIncrement() >= 0) {
                    requests.requestlist.add(dispatchRequest);
                    break;
                }
            }
        } catch (InterruptedException e) {
            log.error("putDispatchRequest interrupted");
        }
    }

    @Override

    /**
     * 不断从内存队列中获取事物消息，并做处理
     */
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                DispatchRequestCollections requests = dispatchRequestBufferQueue.peek();
                if (requests.requestlist.size() > FLOW_CONTROLLER / REQUEST_BUFFER_IN_QUEUE
                        || flushCounter > CHECK_THREAD_LOOP) {
                    this.doFlushDB(false);
                    flushCounter = 0;
                    continue;
                }
                ++flushCounter;
                ThreadUtils.sleep(10);
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * 从内从中获取事物消息，并刷到数据库
     * @param shutdown
     */
    private void doFlushDB(boolean shutdown) {
    	//从队列中获取一个对象
        DispatchRequestCollections requests = dispatchRequestBufferQueue.poll();
        if (requests == null) {
            return;
        }
        //如果不停机 TODO 消费一个消息，补充一个消息
        if (!shutdown) {
            putEmptyRequestList();
        }
        boolean addSuccess = false, removeSuccess = false;
        LinkedHashMap<Long, TransactionRecord> prepareTrs = null;
        LinkedHashMap<Long, Void> confirmTrs = null;
        while (true) {
            if (requests.latch.get() != requests.requestlist.size() && requests.latch.get() > 0) {
                continue;
            }
            requests.latch.set(Integer.MIN_VALUE);

            if (requests.requestlist.size() == 0) {
                break;
            }
            try {
                //数据处理
                if (prepareTrs == null && confirmTrs == null) {
                    prepareTrs = new LinkedHashMap<Long, TransactionRecord>();
                    confirmTrs = new LinkedHashMap<Long, Void>();
                    //从一条都列对象里面的获取一个事物消息集合
                    for (DispatchRequest request : requests.requestlist) {
                    	
                    	//判断事物状态
                        final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
                        switch (tranType) {
                            //不是事物消息
                            case MessageSysFlag.TRANSACTION_NOT_TYPE:
                                break;
                            //事物准备，相当于是第一次提交事物消息    
                            case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                                if (this.maxTransOffset.get() < request.getCommitLogOffset()) {
                                	//放到map中
                                    prepareTrs.put(request.getCommitLogOffset(), new TransactionRecord(request.getCommitLogOffset(),
                                            request.getCheckImmunityTimeOutTimestamp(), request.getMsgSize(), request.getProducerGroup()));
                                    this.maxTransOffset.set(request.getCommitLogOffset());
                                } else {
                                    log.info("[PREPARED] request ignore offset =" + request.getCommitLogOffset());
                                }
                                if (request.getPreparedTransactionOffset() == 0L) {
                                    break;
                                }
                            //TODO  走下面的逻辑 
                            case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                            //如果是回滚消息	
                            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                                if (this.minTransOffset.get() < request.getCommitLogOffset()) {
                                	//去prepareMap中判断，如果有就删除掉
                                    if (prepareTrs.containsKey(request.getPreparedTransactionOffset())) {
                                        prepareTrs.remove(request.getPreparedTransactionOffset());
                                    } else {
                                    	//没有就put
                                        confirmTrs.put(request.getPreparedTransactionOffset(), null);
                                    }
                                } else {
                                    log.info("[COMMIT] request ignore offset =" + request.getCommitLogOffset());
                                }
                                break;
                        }
                    }
                    long transactionOffset = requests.requestlist.get(requests.requestlist.size() - 1).getCommitLogOffset();
                    //设置事物offset
                    transactionOffsetConifgService.putOffset(transactionOffset);
                }

                long startTime = System.currentTimeMillis();
                //插入parpare消息集合
                addSuccess = addSuccess || transactionStore.parpare(new ArrayList<>(prepareTrs.values()));
                //删除回滚消息
                if (addSuccess && (removeSuccess = transactionStore.confirm(new ArrayList<>(confirmTrs.keySet())))) {
                    log.info("pull TransactionRecord consume {}ms ,size={},realParpareSize={},realConfirmSize:{}",
                            (System.currentTimeMillis() - startTime), requests.requestlist.size(), prepareTrs.size(), confirmTrs.size());
                    break;
                }
            } catch (Throwable e) {
                log.error("transactionStore error:", e);
                ThreadUtils.sleep(2000);
            } finally {
                if (addSuccess && removeSuccess) {
                    flowController.release(requests.requestlist.size());
                }
            }
        }
    }


    /**
     * 停止，将内存中的事物消息刷到库中，并且刷配置offset
     */
    public void shutdown() {
        super.shutdown();
        while (dispatchRequestBufferQueue.peek() != null) {
            this.doFlushDB(true);
        }
        transactionOffsetConifgService.persist();
    }

    public TransactionStore getTransactionStore() {
        return transactionStore;
    }

    public long getMaxTransOffset() {
        return maxTransOffset.get();
    }

    public long getMinTransOffset() {
        return minTransOffset.get();
    }
}

