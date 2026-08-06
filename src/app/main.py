import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from .replayer import Replayer

app = FastAPI()

app.state.is_running = False
app.state.replayer_task = False
'''verificar isso Acima'''

''' Verificar os async'''

async def replayer_worker(house_id: str, rate: float):
    
    replayer = Replayer(file_path="src/data/household_power_consumption.txt", house_id=house_id, rate=rate)

    try:

        async for event in replayer.stream_events():

            if not app.state.is_running:
                print("Sinal de Parada")
                break;

            print(f"[KAFKA] Evento processado: {event}")

    except asyncio.CancelledError:
        print("A tarefa em background foi forçada a cancelar.")
    except Exception as e:
        print(f"Erro no processamento do replayer: {e}")
    finally:
        app.state.is_running = False


@app.get("/health", tags=["Monitoramento"])
async def health_check():
    return {
        "status": "healthy",
        "replayer_running": app.state.is_running
    }

class StartConfig(BaseModel):
    # Valores default para facilitar o teste na API
    house_id: str = "HOUSE_001"
    rate: float = 1.0

    
@app.post("/start", tags=["Controle"])
async def start_replayer(config: StartConfig):

    if app.state.is_running:
        raise HTTPException(status_code=400, detail="O Replayer já está em execução.")

    app.state.is_running = True

    app.state.replayer_task = asyncio.create_task(
        replayer_worker(house_id=config.house_id, rate=config.rate)
    )

    return {"message": "Replayer iniciado com sucesso em background.", "config": config}


@app.post("/pause", tags=["Controle"])
async def pause_replayer():

    if not app.state.is_running:
       return {"message": "O Replayer não está rodando no momento."}

    app.state.is_running = False

    return {"message": "Sinal de pausa enviado. O loop irá parar no próximo ciclo."}
