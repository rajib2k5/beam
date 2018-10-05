package org.apache.beam.runners.flink;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.NativeTransforms;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.flink.FlinkStreamingPortablePipelineTranslator.PTransformTranslator;
import org.apache.beam.runners.flink.FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TestStreamingPortableTranslation {
  private static final String TEST_URN = "mwylde:testStreamingInput";

  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class IsFlinkNativeTransform implements NativeTransforms.IsNativeTransform {

    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return TEST_URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }

  public void addTo(
      ImmutableMap.Builder<String, PTransformTranslator<StreamingTranslationContext>>
          translatorMap) {
    translatorMap.put(TEST_URN, this::translateTestInput);
  }

  private void translateTestInput(String id,
      RunnerApi.Pipeline pipeline,
      FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context) {

    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);


    DataStreamSource<WindowedValue<byte[]>> source = context
        .getExecutionEnvironment().addSource(
            new RichParallelSourceFunction<WindowedValue<byte[]>>() {
              private AtomicBoolean cancelled = new AtomicBoolean(false);

              @Override
              public void run(SourceContext<WindowedValue<byte[]>> ctx) throws Exception {
                int counter = 0;
                while (!cancelled.get()) {
                  String value = Thread.currentThread().getId() + "-" + counter;
                  counter++;
                  ctx.collect(WindowedValue.valueInGlobalWindow(
                      value.getBytes(Charset.forName("UTF-8"))));
                  Thread.sleep(10);
                }
              }

              @Override
              public void cancel() {
                cancelled.set(true);
              }
              });

    context.addDataStream(Iterables.getOnlyElement(pTransform.getOutputsMap().values()), source);
  }
}
