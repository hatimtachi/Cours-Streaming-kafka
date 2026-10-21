package fr.esgi.kafka.dispatch.rest;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Bonus DISP-6 : exposer le surge courant par zone via Interactive Queries.
 * Piste : injecter org.apache.kafka.streams.KafkaStreams (bean fourni par
 * l'extension quarkus-kafka-streams) puis interroger le state store du
 * ticket DISP-4 avec streams.store(StoreQueryParameters.fromNameAndType(...)).
 */
@Path("/api/v1/surge")
@Produces(MediaType.APPLICATION_JSON)
public class SurgeResource {

    // @Inject
    // org.apache.kafka.streams.KafkaStreams streams;

    @GET
    @Path("/{zone}")
    public Response surge(@PathParam("zone") String zone) {
        return Response.status(Response.Status.NOT_IMPLEMENTED)
                .entity("{\"error\": \"A implementer (ticket DISP-6)\"}")
                .build();
    }
}
