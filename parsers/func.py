class get_data():
 def get_data(self,target_datatime,Session,url,Format:str = None):
    if target_datatime:
        raise NotImplementedError("This parser is not yet able to parse past dates")
    r = get(url)
    if ~Format:
        if Format =='json':
            r = r.json()
        if Format=='row':
            r = r.row()
    return r