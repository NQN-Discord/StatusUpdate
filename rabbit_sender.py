from rabbit_helper import Rabbit


class StatusUpdateRabbit(Rabbit):
    @Rabbit.sender("GATEWAY_STATUS_UPDATE", 0)
    def send_status(self, status: str):
        return {"status": status}
