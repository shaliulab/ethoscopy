
def load_hour_start(date):
    t = date
    t = t.split(' ')
    hh, mm, ss = map(int, t[1].split(':'))
    return hh
    #return  hh # + mm/60 + ss/3600

def compute_t_after_ref(date, reference_hour):
    hour_start=load_hour_start(date)
    t_after_ref = ((hour_start - reference_hour) % 24) * 3600
    return t_after_ref