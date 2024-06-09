def strategy_id_generator(begin=1):
    i = begin
    while(True):
        yield i
        i += 1
