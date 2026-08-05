import asyncio
from datetime import datetime, timezone
from typing import AsyncGenerator

class Replayer:
    def __init__(self, file_path: str, house_id: str, rate: float = 1.0):

        self.file_path = file_path
        self.house_id = house_id
        self.delay = 1.0 / rate if rate > 0 else 0

    
    async def stream_events(self) ->AsyncGenerator[dict, None]:

        with open(self.file_path, "r", encoding="utf-8") as file:
            
            cabecalho = file.readline().strip().split(";")

            for linha in file:
                data = linha.strip().split(";")
                line = dict(zip(cabecalho, data))

                line_cleaned = self.parse_line(line)

                event = {
                    "house_id": self.house_id,
                    "Timestamp": datetime.now(timezone.utc).isoformat(),
                    "metrics": line_cleaned
                }

                yield event

                await asyncio.sleep(self.delay)


    def parse_line(self, row: dict) -> dict:

        data = {}
        for key, value in row.items():

            if key == 'Date' or key == 'Time':
                continue

            if value == '?':
                 data[key] = None

            else:

                try:
                    data[key] = float(value)
                except (ValueError, TypeError):
                    data[key] = value
        
        return data