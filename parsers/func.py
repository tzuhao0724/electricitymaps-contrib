from requests import Session
class get_data():
    def get_data(self,session=None,url:str=" ",Format = None):
        r= session or Session()
        r = r.get(url)
        if Format !=None:
            if Format =='json':
                r = r.json()

            if Format=='raw':
                r = r.raw()
        return r
    def get_data_warn(self,session=None,url:str=" ",Format:str = None,target_datetime=None):
        if target_datetime is not None:
            raise NotImplementedError("This parser is not yet able to parse past dates")
        r= session or Session()
        r = r.get(url)
        if Format !=None:
            if Format =='json':
                r = r.json()
            if Format=='raw':
                r = r.raw()
        return r
