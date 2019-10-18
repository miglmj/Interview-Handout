import mock_db
import uuid
from worker import worker_main
from threading import Thread
from time import sleep, time

LOCK_SENTINEL_KEY = "WRITING_BLOCKED"
RETRY = False


def lock_is_free():
    """
        CHANGE ME, POSSIBLY MY ARGS

        Return whether the lock is free
    """
    return db.find_one({"_id": LOCK_SENTINEL_KEY}) is None


def set_lock(id):
    """
        Try attaining a lock to write to the database
        Lock is "attained" by succesfully writing this value to the db
        Inserting throws an exception if the key is already present,
        so taking the lock fails
    """
    try:
        db.insert_one({"_id": LOCK_SENTINEL_KEY, "locked": True})
        return True
    except Exception:
        return False

def release_lock(id):
    """
        Release lock by deleting the record in db, so other writes with the same
        key to the db will be succesful.
    """
    db.delete_one({"_id": LOCK_SENTINEL_KEY})

def attempt_run_worker(worker_hash, give_up_after, db, retry_interval):
    """
        CHANGE MY IMPLEMENTATION, BUT NOT FUNCTION SIGNATURE

        Run the worker from worker.py by calling worker_main

        Args:
            worker_hash: a random string we will use as an id for the running worker
            give_up_after: if the worker has not run after this many seconds, give up
            db: an instance of MockDB
            retry_interval: continually poll the locking system after this many seconds
                            until the lock is free, unless we have been trying for more
                            than give_up_after seconds
    """

    start_time = time()
    done = False
    while not done:
        giveup = time() - start_time > give_up_after
        if giveup:
            # Could do something to keep track of timeouts here
            break
        if lock_is_free() and set_lock(worker_hash):
            try:
                worker_main(worker_hash, db)
                done = True
            except Exception:
                done = not RETRY
            finally:
                release_lock(worker_hash)
        else:
            sleep(retry_interval)


if __name__ == "__main__":
    """
        DO NOT MODIFY

        Main function that runs the worker five times, each on a new thread
        We have provided hard-coded values for how often the worker should retry
        grabbing lock and when it should give up. Use these as you see fit, but
        you should not need to change them
    """

    db = mock_db.DB()
    threads = []
    for _ in range(25):
        t = Thread(target=attempt_run_worker, args=(uuid.uuid1(), 2000, db, 0.1))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
