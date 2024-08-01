import { AlreadyClosedError } from './alreadyClosedError';
import { Dequeue } from './dequeue';
import { Quota } from './quota/quota';
import { QuotaManager } from './quota/quotaManager';
import { RateLimitTimeoutError } from './rateLimitTimeoutError';

export function pRateLimit(
  quotaManager: QuotaManager | Quota
): {
  /** Function to run a function under the limiter. */
  limiter: <T>(fn: () => Promise<T>) => Promise<T>;
  /**
   * Function used to cleanup the rate limiter and associated quota manager.
   * After cleaning up, the rate limiter can no longer be used.
   */
  cleanup: () => Promise<void>;
} {
  if (!(quotaManager instanceof QuotaManager)) {
    return pRateLimit(new QuotaManager(quotaManager));
  }

  let isClosed = false;
  const cleanupFn = async () => {
    if (isClosed) return;
    quotaManager.close();
    isClosed = true;
  };

  const queue = new Dequeue<Function>();
  let timerId: NodeJS.Timer = null;

  const next = () => {
    while (!isClosed && queue.length && quotaManager.start()) {
      queue.shift()();
    }

    if (queue.length && !quotaManager.activeCount && !timerId) {
      timerId = setTimeout(() => {
        timerId = null;
        next();
      }, 100);
    }
  };

  const limiter = <T>(fn: () => Promise<T>) => {
    return new Promise<T>((resolve, reject) => {
      if (isClosed)
        throw new AlreadyClosedError('Limiter has been closed and cannot be used');

      let timerId: NodeJS.Timer = null;
      if (quotaManager.maxDelay) {
        timerId = setTimeout(() => {
          timerId = null;
          reject(new RateLimitTimeoutError('queue maxDelay timeout exceeded'));
          next();
        }, quotaManager.maxDelay);
      }

      const run = () => {
        if (quotaManager.maxDelay) {
          if (timerId) {
            clearTimeout(timerId);
          } else {
            // timeout already fired
            return;
          }
        }

        fn()
          .then((val) => {
            quotaManager.end();
            resolve(val);
          })
          .catch((err) => {
            quotaManager.end();
            reject(err);
          })
          .then(() => {
            next();
          });
      };

      queue.push(run);
      next();
    });
  };

  return { limiter, cleanup: cleanupFn };
}
