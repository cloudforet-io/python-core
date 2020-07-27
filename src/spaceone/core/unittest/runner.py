from xmlrunner import XMLTestRunner
from spaceone.core.unittest.result import RichTestResult

UTF8 = 'UTF-8'


class RichTestRunner(XMLTestRunner):
    """
    A test runner class that outputs the results in JUnit like XML files.
    """

    def __init__(self, output='reports', outsuffix=None,
                 elapsed_times=True, encoding=UTF8,
                 resultclass=RichTestResult, 
                 **kwargs):
        super(RichTestRunner, self).__init__(output, outsuffix,
                                             elapsed_times, encoding, resultclass, **kwargs)
