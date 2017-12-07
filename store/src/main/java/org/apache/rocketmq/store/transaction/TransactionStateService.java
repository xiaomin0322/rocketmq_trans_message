package org.apache.rocketmq.store.transaction;

import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DispatchRequest;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author fengjian
 * @version V1.0
 * @title: rocketmq-all
 * @Package org.apache.rocketmq.store.transaction
 * @Description: 事务接口
 * @date 2017/11/3 下午4:17
 */
public interface TransactionStateService {

	/**
	 * 初始化加载
	 * @return
	 */
    boolean load();

    /**
     * 开始
     */
    void start();

    /**
     * 停止
     */
    void shutdown();

    /**
     * 删除失效状态文件
     * @param offset
     * @return
     */
    int deleteExpiredStateFile(long offset);

    void recoverStateTable(final boolean lastExitOK);
    /**新增事物消息方法入口
     * 
     * @param request
     * @return
     */
    boolean appendPreparedTransaction(DispatchRequest request);
    /**修改事物消息方法入口
     * 
     * @param request
     * @return
     */
    boolean updateTransactionState(DispatchRequest request);

    ConsumeQueue getTranRedoLog();
    /**
     * 获取本地消息的offset
     * @return
     */
    AtomicLong getTranStateTableOffset();
    /**
     * 最大事物的offset
     * @return
     */
    public long getMaxTransOffset();
   /**最大事物的offset
    * 
    * @return
    */
    public long getMinTransOffset();
}
