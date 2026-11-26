import json
from concurrent import futures

import grpc
import mysql.connector
import event_pb2
import event_pb2_grpc
import registration_pb2 as pb
import registration_pb2_grpc as rpc

CONFIG = json.load(open("config.json"))


def read(sql, args=()):
    with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor(dictionary=True) as cursor:
        cursor.execute(sql, args)
        return cursor.fetchall()


def write(sql, args=()):
    with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor() as cursor:
        cursor.execute(sql, args)
        db.commit()
        return cursor.lastrowid


def call(method, request, context):
    try:
        return method(request)
    except grpc.RpcError as error:
        context.abort(error.code(), error.details())


class RegistrationService(rpc.RegistrationServiceServicer):
    def __init__(self, events):
        self.events = events

    def ListBookings(self, request, context):
        bookings = read(
            """SELECT b.booking_id, b.event_id, b.participant_id,
                      p.name AS participant_name
               FROM bookings b
               JOIN participants p
                 ON b.participant_id = p.participant_id
               WHERE b.event_id = %s
                 AND b.state = 'confirmed'
               ORDER BY b.booking_id ASC""",
            (request.event_id,)
        )

        return pb.ListBookingsResponse(
            bookings=[pb.Booking(**booking) for booking in bookings]
        )

    def CreateBooking(self, request, context):
        event_id = request.event_id
        participant_id = request.participant_id

        if event_id <= 0 or participant_id <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid event or participant ID")

        participant = read(
            "SELECT name FROM participants WHERE participant_id = %s",
            (participant_id,)
        )

        if not participant:
            context.abort(grpc.StatusCode.NOT_FOUND, "Participant not found")

        existing = read(
            """SELECT booking_id FROM bookings
               WHERE event_id = %s AND participant_id = %s
               AND state = 'confirmed'""",
            (event_id, participant_id)
        )

        if existing:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "Booking already exists")

        call(
            self.events.ReserveSeat,
            event_pb2.EventId(event_id=event_id),
            context
        )

        try:
            booking_id = write(
                """INSERT INTO bookings (event_id, participant_id, state)
                   VALUES (%s, %s, 'confirmed')""",
                (event_id, participant_id)
            )
        except mysql.connector.Error:
            call(
                self.events.ReleaseSeat,
                event_pb2.EventId(event_id=event_id),
                context
            )
            context.abort(grpc.StatusCode.INTERNAL, "Could not create booking")

        return pb.Booking(
            booking_id=booking_id,
            event_id=event_id,
            participant_id=participant_id,
            participant_name=participant[0]["name"]
        )

    def CancelBooking(self, request, context):
        found = read("SELECT event_id, state FROM bookings WHERE booking_id = %s", (request.booking_id,))
        if not found:
            context.abort(grpc.StatusCode.NOT_FOUND, "Booking not found")
        if found[0]["state"] == "confirmed":
            write("UPDATE bookings SET state = 'cancelled' WHERE booking_id = %s", (request.booking_id,))
            call(self.events.ReleaseSeat, event_pb2.EventId(event_id=found[0]["event_id"]), context)
        return pb.Empty()

    def CreateParticipant(self, request, context):
        name = request.name.strip()
        if not name or len(name) > 150:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Name must be 1-150 characters")
        participant_id = write("INSERT INTO participants (name) VALUES (%s)", (name,))
        return pb.Participant(participant_id=participant_id, name=name)

    def GetParticipant(self, request, context):
        found = read("SELECT participant_id, name FROM participants WHERE participant_id = %s",
                     (request.participant_id,))
        if not found:
            context.abort(grpc.StatusCode.NOT_FOUND, "Participant not found")
        return pb.Participant(**found[0])


events = event_pb2_grpc.EventServiceStub(grpc.insecure_channel(CONFIG["event_service"]))
server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
rpc.add_RegistrationServiceServicer_to_server(RegistrationService(events), server)
server.add_insecure_port(CONFIG["listen"])
server.start()
print(f"Registration service on {CONFIG['listen']}", flush=True)
server.wait_for_termination()
