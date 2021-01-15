package reactivejson;


import com.fasterxml.jackson.core.async_.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ReactorObjectReader {

    private final JsonFactory jsonFactory;

    public ReactorObjectReader(JsonFactory jsonFactory) {
        this.jsonFactory = jsonFactory;
    }

    public <T> Flux<T> readElements(Publisher<ByteBuffer> input, ObjectReader objectReader) {
        try {
            NonBlockingObjectReader nonBlockingObjectReader = new NonBlockingObjectReader(
                    jsonFactory, true, objectReader);

            return readImpl(input, nonBlockingObjectReader);
        } catch (IOException ex) {
            return Flux.error(ex);
        }
    }

    public Mono<JsonNode> readTree(Publisher<ByteBuffer> input, ObjectReader objectReader) {
        try {
            NonBlockingObjectReader nonBlockingObjectReader = new NonBlockingObjectReader(
                    jsonFactory, false, objectReader);

            return this.readTreeImpl(input, nonBlockingObjectReader);
        } catch (IOException ex) {
            return Mono.error(ex);
        }
    }

    public <T> Mono<T> read(Publisher<ByteBuffer> input, ObjectReader objectReader) {
        try {
            NonBlockingObjectReader nonBlockingObjectReader = new NonBlockingObjectReader(
                    jsonFactory, false, objectReader);

            return this.<T>readImpl(input, nonBlockingObjectReader).singleOrEmpty();
        } catch (IOException ex) {
            return Mono.error(ex);
        }
    }

    private Mono<JsonNode> readTreeImpl(Publisher<ByteBuffer> input, NonBlockingObjectReader reader) {
        return Mono.from(input).flatMap(
                byteBuffer -> {
                    try {
                        return Mono.just(reader.readTree(byteBuffer));
                    } catch (IOException e) {
                        return Mono.error(e);
                    }
                }
        );
    }

    private <T> Flux<T> readImpl(Publisher<ByteBuffer> input, NonBlockingObjectReader reader) {
        return Flux.concat(
                Flux.from(input).concatMap(
                        byteBuffer -> {
                            try {
                                return Flux.fromIterable(reader.readObjects(byteBuffer));
                            } catch (IOException e) {
                                return Flux.error(e);
                            }
                        }),
                Flux.defer(() -> {
                    try {
                        return Flux.fromIterable(reader.endOfInput());
                    } catch (IOException e) {
                        return Flux.error(e);
                    }
                }));
    }
}
