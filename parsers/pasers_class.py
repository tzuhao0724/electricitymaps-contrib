import abc


class paser_base (abc.ABC)
    @abc.abstractmethod
    def fetch_price(
        zone_key: str,
    session: Session = Session(),
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
    ) -> dict:
        pass

    @abc.abstractmethod
    def fetch_production(
    zone_key: str ,
    session: Session = Session(),
    target_datetime: Optional[datetime] = None,
    logger: Logger =getLogger(__name__),
    ) -> dict:
        pass
    
    @abc.abstractmethod
    def fetch_consumption_forecast(
    zone_key: str,
    session: Session = Session(),
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
    )   -> dict:
        pass
    
    @abc.abstractmethod
    def fetch_exchange(
    zone_key1: str,
    zone_key2: str,
    session: Session = Session(),
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
    ) -> dict:
        pass
    
    @abc.abstractmethod
    
