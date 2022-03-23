package StreamLedger.functions;

import StreamLedger.data.DepositEvent;
import StreamLedger.data.TransactionEvent;
import StreamLedger.data.TransactionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.types.Either;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import redis.clients.jedis.JedisPooled;

import java.io.Serializable;

public class TransferDepositHandler extends RichMapFunction<Either<DepositEvent, TransactionEvent>, TransactionResult> {

//    private final JedisPooled jedis;
//    private final RLock lock;

    public TransferDepositHandler() {}

    @Override
    public TransactionResult map(Either<DepositEvent, TransactionEvent> depositOrTransferEvent) throws Exception {
        if (depositOrTransferEvent.isRight()) {
            return transfer(depositOrTransferEvent.right());
        }
        else {
            return deposit(depositOrTransferEvent.left());
        }
    }

    public TransactionResult transfer(TransactionEvent event) {
        // create lock for state access
        RedissonClient redisson = Redisson.create();
        RLock lock = redisson.getLock("lock");

        TransactionResult transactionResult = null;

        // lock before executing transaction
        lock.lock();

        final long sourceAccountBalance = get(event.getSourceAccountId());
        final long sourceAssetValue = get(event.getSourceBookEntryId());
        final long targetAccountBalance = get(event.getTargetAccountId());
        final long targetAssetValue = get(event.getTargetBookEntryId());

        // check the preconditions
        if (sourceAccountBalance > event.getMinAccountBalance()
                && sourceAccountBalance > event.getAccountTransfer()
                && sourceAssetValue > event.getBookEntryTransfer()) {

            // compute the new balances
            final long newSourceBalance = sourceAccountBalance - event.getAccountTransfer();
            final long newTargetBalance = targetAccountBalance + event.getAccountTransfer();
            final long newSourceAssets = sourceAssetValue - event.getBookEntryTransfer();
            final long newTargetAssets = targetAssetValue + event.getBookEntryTransfer();

            // write back the updated values
            set(event.getSourceAccountId(), newSourceBalance);
            set(event.getTargetAccountId(), newTargetBalance);
            set(event.getSourceBookEntryId(), newSourceAssets);
            set(event.getTargetBookEntryId(), newTargetAssets);

            transactionResult = new TransactionResult(event, true, newSourceBalance, newTargetBalance);
        } else {
            // emit result with unchanged balances and a flag to mark transaction as rejected
            transactionResult = new TransactionResult(event, false, sourceAccountBalance, targetAccountBalance);
        }

        lock.unlock();
        redisson.shutdown();
        return transactionResult;
    }

    public TransactionResult deposit(DepositEvent event) {
        // create lock for state access
        RedissonClient redisson = Redisson.create();
        RLock lock = redisson.getLock("lock");
        // init redis client
        lock.lock();

        long newAccountValue = get(event.getAccountId()) + event.getAccountTransfer();
        long newAssetValue = get(event.getBookEntryId()) + event.getBookEntryTransfer();

        set(event.getAccountId(), newAccountValue);
        set(event.getBookEntryId(), newAssetValue);

        lock.unlock();
        redisson.shutdown();
        return new TransactionResult();
    }

    public long get(String key) {
        JedisPooled jedis = new JedisPooled("localhost", 6379);
        String value = jedis.get(key);
        long ret = jedis.get(key) == null ? 0L : Long.parseLong(value);
        jedis.close();
        return ret;
    }

    public void set(String key, long value) {
        JedisPooled jedis = new JedisPooled("localhost", 6379);
        jedis.set(key, String.valueOf(value));
        jedis.close();
    }
}
