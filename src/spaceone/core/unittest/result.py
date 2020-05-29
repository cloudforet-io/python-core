# -*- coding: utf-8 -*-
import os
import sys
import pprint
import json
from xmlrunner.result import _XMLTestResult
from xmlrunner.result import _TestInfo

try:
    from google.protobuf.json_format import MessageToDict
except Exception as e:
    print(f'Failed to import: {str(e)}')

_PP = pprint.PrettyPrinter(indent=4)


def print_data(data, description=None):
    if os.environ.get('TEST_DEBUG', 'false') == 'true':
        print()
        if description:
            print(f'[ {description} ]')

        _PP.pprint(data)


def print_json(json_data, description=None):
    data = json.loads(json_data)
    print_data(data, description)


def print_message(message, description=None):
    data = MessageToDict(message, preserving_proto_field_name=True)
    print_data(data, description)


def testcase_name(test_method):
    fullpath = (test_method.id()).split('.')
    return fullpath[-1]


class RichTestInfo(_TestInfo):
    # Possible test outcomes
    (SUCCESS, FAILURE, ERROR, SKIP) = range(4)

    def __init__(self, test_result, test_method, outcome=SUCCESS, err=None, subTest=None, filename=None, lineno=None):
        super(RichTestInfo, self).__init__(test_result, test_method, outcome, err, subTest, filename, lineno)
        self.short_id = testcase_name(test_method)

    def result(self):
        if self.outcome == 0:
            result = "SUCCESS"
        else:
            result = (RichTestInfo.OUTCOME_ELEMENTS[self.outcome]).upper()
        return '{:<40s}{:<30s}{:>20s}{:>20f}'.format(self.short_id, self.test_name, result, self.elapsed_time)


class RichTestResult(_XMLTestResult):
    """
    A test result class that can express test results in a XML report.

    Used by XMLTestRunner.
    """

    def __init__(self, stream=sys.stderr, descriptions=1, verbosity=1,
                 elapsed_times=True, properties=None, infoclass=RichTestInfo):
        super(RichTestResult, self).__init__(stream, descriptions, verbosity, elapsed_times, properties, infoclass)

    def _prepare_callback(self, test_info, target_list, verbose_str,
                          short_str):
        """
            Appends a `infoclass` to the given target list and sets a callback
            method to be called by stopTest method.
            """
        test_info.filename = self.filename
        test_info.lineno = self.lineno
        target_list.append(test_info)

        def callback():
            """Prints the test method outcome to the stream, as well as
                the elapsed time.
                """

            test_info.test_finished()

            # Ignore the elapsed times for a more reliable unit testing
            if not self.elapsed_times:
                self.start_time = self.stop_time = 0

            self.stream.writeln(test_info.result())
            self.stream.flush()

        self.callback = callback
