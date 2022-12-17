import sys


class FinanceException(Exception):
    def __init__(self, error_message: Exception, error_detail: sys):
        """
        :param error_message: Error message in string format.
        """
        super().__init__(error_message)

        self.error_message = FinanceException.error_message_detail(
            error_message=error_message, error_detail=error_detail
        )

    @staticmethod
    def error_message_detail(error_message: Exception, error_detail: sys) -> str:
        """
        :param error_message: Exception object raises from the module.
        :param error_detail: Contain detailed information about system execution information.
        """
        _, _, exec_tb = error_detail.exc_info()
        line_number = exec_tb.tb_lineno
        file_name = exec_tb.tb_frame.f_code.co_filename
        return "Error occurred! Python script name [{0}]; line number [{1}]; error message [{2}]".format(
            file_name, line_number, error_message
        )

    def __str__(self):
        """
        Formatting an object to be visible if used in the print statement.
        """
        return self.error_message

    def __repr__(self) -> str:
        """
        Formatting object of FinanceException
        """
        return str(FinanceException.__name__)
