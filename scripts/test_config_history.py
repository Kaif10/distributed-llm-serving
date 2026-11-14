import unittest
from config_history import check_history


def write(id,start,end,expected=0,status=200,revision=1):
    return {"id":id,"call_ns":start,"return_ns":end,"method":"PUT","path":"/config",
            "request":{"request_id":id,"expected_revision":expected,"workers":[]},"status":status,
            "response":{"revision":revision,"workers":[]} if status==200 else "error"}


def read(id,start,end,revision):
    return {"id":id,"call_ns":start,"return_ns":end,"method":"GET","path":"/config","request":None,
            "status":200,"response":{"revision":revision,"workers":[]}}


class ConfigHistoryTests(unittest.TestCase):
    def test_accepts_concurrent_cas_conflict(self):
        self.assertTrue(check_history([write("a",1,4),write("b",2,3,status=409),read("r",5,6,1)])["linearizable"])

    def test_catches_two_successes_for_same_revision(self):
        self.assertFalse(check_history([write("a",1,4),write("b",2,3)])["linearizable"])

    def test_catches_stale_read_after_completed_write(self):
        self.assertFalse(check_history([write("a",1,2),read("r",3,4,0)])["linearizable"])

    def test_catches_acknowledged_write_lost_at_restart(self):
        self.assertFalse(check_history([write("a",1,2),read("after-restart",10,11,0)])["linearizable"])

    def test_original_retry_result_can_differ_from_current(self):
        retry=write("a",5,6)
        self.assertTrue(check_history([write("a",1,2),write("b",3,4,expected=1,revision=2),retry,read("r",7,8,2)])["linearizable"])
        retry["response"]["revision"]=2
        self.assertFalse(check_history([write("a",1,2),write("b",3,4,expected=1,revision=2),retry])["linearizable"])

    def test_unknown_write_can_commit_after_its_error_response(self):
        history=[write("unknown",1,2,status=503),read("before-commit",3,4,0),read("after-commit",5,6,1)]
        self.assertTrue(check_history(history)["linearizable"])

    def test_search_limit_is_inconclusive_not_success(self):
        with self.assertRaises(RuntimeError):
            check_history([write("a",1,4),write("b",2,3)],max_states=1)

    def test_malformed_timing_or_read_status_is_not_evidence(self):
        with self.assertRaises(ValueError):check_history([read("r",2,1,0)])
        invalid=read("r",1,2,0);invalid["status"]=409
        with self.assertRaises(ValueError):check_history([invalid])


if __name__=="__main__":
    unittest.main()
