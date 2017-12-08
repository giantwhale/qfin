def parse_result(res):
    """Parse Result returned from GDAX Exchange, 
    In case of bad result, return None instead of the original message,
    so the program won't creash
    @ToDo: implement me!
    """
    if isinstance(res, dict) and res.get('message', '') == 'NotFound':
        # is this correct?
        return None
    return res
