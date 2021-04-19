from qc_dimensions import *
from qc_facts import *
from qc_views import *

class AMEX():
    '''
    Organizing dimension and fact functions into class.
    '''
    def dimensions(self):
        qc_dimensions()

    def facts(self):
        qc_facts()

    def summary_unmapped(self):
        return summ_unmapped()

    # def summary_mapped(self):
    #     return summ_mapped()

    def final_view(self):
        return final_view()
