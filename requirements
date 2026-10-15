"""
Skybound multiplayer room server.
Requires Python 3.10+ and: pip install websockets
Run: python server.py
By default listens on port 8765. For friends outside your network, host this server
on a public Python host and configure the browser client to use its WebSocket URL.
"""
import asyncio, json, secrets, string
import websockets

rooms = {}

def room_code():
    alphabet = string.ascii_uppercase + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(6))

async def handler(websocket):
    room = None
    player_id = None
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            kind = msg.get("type")
            if kind == "join":
                requested = str(msg.get("room", "")).upper()[:8]
                if not requested:
                    requested = room_code()
                room = requested
                player_id = str(msg.get("id", secrets.token_hex(4)))[:32]
                rooms.setdefault(room, {})
                # Replace a stale connection with same id if needed.
                rooms[room][player_id] = {
                    "id": player_id,
                    "name": str(msg.get("name", "Player"))[:16],
                    "x": 0, "y": 1, "z": 0, "socket": websocket
                }
                await websocket.send(json.dumps({"type": "joined", "code": room}))
                await broadcast(room)
            elif kind == "move" and room and player_id and room in rooms:
                p = rooms[room].get(player_id)
                if p:
                    # Clamp coordinates to keep payloads sensible.
                    for axis in ("x", "y", "z"):
                        try:
                            p[axis] = max(-500, min(500, float(msg.get(axis, 0))))
                        except (TypeError, ValueError):
                            p[axis] = 0
                    await broadcast(room)
    except websockets.ConnectionClosed:
        pass
    finally:
        if room and player_id and room in rooms:
            rooms[room].pop(player_id, None)
            if not rooms[room]:
                rooms.pop(room, None)
            else:
                await broadcast(room)

async def broadcast(room):
    players = []
    for p in rooms.get(room, {}).values():
        players.append({k: p[k] for k in ("id", "name", "x", "y", "z")})
    payload = json.dumps({"type": "state", "players": players})
    sockets = [p["socket"] for p in rooms.get(room, {}).values()]
    if sockets:
        await asyncio.gather(*(s.send(payload) for s in sockets), return_exceptions=True)

async def main():
    print("Skybound room server listening on ws://0.0.0.0:8765")
    async with websockets.serve(handler, "0.0.0.0", 8765, max_size=8192):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
