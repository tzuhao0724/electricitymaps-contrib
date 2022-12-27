from requests import Session
class get_data():
    def get_data(self,session=None,url:str=" ",Format:str = None):
        r= session or Session()
        r = r.get(url)
        if Format !=None:
            if Format =='json':
                r = r.json()
            if Format=='raw':
                r = r.raw()
        return r
