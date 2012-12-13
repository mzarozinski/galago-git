/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Coordinate ascent learning algorithm.
 *
 * This class implements the linear ranking model known as Coordinate Ascent. It
 * was proposed in this paper: D. Metzler and W.B. Croft. Linear feature-based
 * models for information retrieval. Information Retrieval, 10(3): 257-274,
 * 2000.
 *
 *
 * issues - may want different step sizes -> possibly as a fraction of the range
 * of a variable (e.g. mu)
 *
 * @author bemike
 * @author sjh
 */
public class CoordinateAscentLearner extends Learner {
  // this is the max step size

  private static final double MAX_STEP = Math.pow(10, 6);
  // coord ascent specific parameters
  protected int maxIterations;
  protected HashMap<String, Double> minStepSizes;
  protected double minStepSize;
  protected double maxStepRatio;
  protected double stepScale;

  public CoordinateAscentLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    this.maxStepRatio = p.get("maxStepRatio", 0.5);
    this.stepScale = p.get("stepScale", 2.0);
    this.maxIterations = (int) p.get("maxIterations", 5);
    this.minStepSizes = new HashMap();
    this.minStepSize = p.get("minStepSize", 0.02);
    Parameters specialMinStepSizes = new Parameters();
    if (p.isMap("specialMinStepSize")) {
      specialMinStepSizes = p.getMap("specialMinStepSize");
    }
    for (String param : learnableParameters.getParams()) {
      minStepSizes.put(param, specialMinStepSizes.get(param, this.minStepSize));
    }
  }

  @Override
  public RetrievalModelInstance learn() throws Exception {
    // loop for each random restart:
    final List<RetrievalModelInstance> learntParams = Collections.synchronizedList(new ArrayList());

    if (threading) {
      ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
      final List<Exception> exceptions = Collections.synchronizedList(new ArrayList());
      final CountDownLatch latch = new CountDownLatch(restarts);

      for (int i = 0; i < restarts; i++) {
        final RetrievalModelInstance settingsInstance;
        if (initialSettings.size() > i) {
          settingsInstance = initialSettings.get(i).clone();
        } else {
          settingsInstance = generateRandomInitalValues();
        }
        settingsInstance.setAnnotation("name", name + "-randomStart-" + i);


        Thread t = new Thread() {
          @Override
          public void run() {
            try {
              RetrievalModelInstance s = runCoordAscent(settingsInstance);
              s.setAnnotation("score", Double.toString(evaluate(s)));
              learntParams.add(s);
              synchronized (outputPrintStream) {
                outputPrintStream.println(s.toString());
                outputPrintStream.flush();
              }
            } catch (Exception e) {
              exceptions.add(e);
              synchronized (outputTraceStream) {
                outputTraceStream.println(e.toString());
                outputTraceStream.flush();
              }
            } finally {
              latch.countDown();
            }
          }
        };
        threadPool.execute(t);
      }

      while (latch.getCount() > 0) {
        logger.info(String.format("Waiting for %d threads.", latch.getCount()));
        try {
          latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          // do nothing
        }
      }

      threadPool.shutdown();

      if (!exceptions.isEmpty()) {
        for (Exception e : exceptions) {
          System.err.println("Caught exception: \n" + e.toString());
          e.printStackTrace();
        }
      }

    } else {

      for (int i = 0; i < restarts; i++) {
        final RetrievalModelInstance settingsInstance;
        if (initialSettings.size() > i) {
          settingsInstance = initialSettings.get(i).clone();
        } else {
          settingsInstance = generateRandomInitalValues();
        }
        settingsInstance.setAnnotation("name", name + "-randomStart-" + i);
        try {
          RetrievalModelInstance s = runCoordAscent(settingsInstance);
          s.setAnnotation("score", Double.toString(evaluate(s)));
          learntParams.add(s);
          synchronized (outputPrintStream) {
            outputPrintStream.println(s.toString());
            outputPrintStream.flush();
          }
        } catch (Exception e) {
          System.err.println("Caught exception: \n" + e.toString());
          e.printStackTrace();
          synchronized (outputTraceStream) {
            outputTraceStream.println(e.toString());
            outputTraceStream.flush();
          }
        }
      }
    }

    // check if we have learnt some values
    if (learntParams.isEmpty()) {
      return generateRandomInitalValues();
    } else {
      RetrievalModelInstance best = learntParams.get(0);
      double bestScore = Double.parseDouble(best.getAnnotation("score"));

      for (RetrievalModelInstance inst : learntParams) {
        double score = Double.parseDouble(inst.getAnnotation("score"));
        if(bestScore < score){
          best = inst;
          bestScore = score;
        }
      }
      
      best.setAnnotation("name", name + "-best");
      
      outputPrintStream.println(best.toString());
      outputPrintStream.flush();
      
      return best;
    }
  }

  public RetrievalModelInstance runCoordAscent(RetrievalModelInstance parameterSettings) throws Exception {

    double best = this.evaluate(parameterSettings);
    outputTraceStream.println(String.format("Initial parameter weights: %s Metric: %f. Starting optimization...", parameterSettings.toParameters().toString().toString(), best));

    boolean optimized = true;
    int iters = 0;
    while (optimized && iters < maxIterations) {
      List<String> optimizationOrder = new ArrayList(this.learnableParameters.getParams());
      Collections.shuffle(optimizationOrder, this.random);
      outputTraceStream.println(String.format("Starting a new coordinate sweep...."));
      iters += 1;
      optimized = false;

      for (int c = 0; c < optimizationOrder.size(); c++) { // outer iteration
        String coord = optimizationOrder.get(c);
        outputTraceStream.println(String.format("Iteration (%d of %d). Step (%d of %d). Starting to optimize coordinate (%s)...", iters, this.maxIterations, c + 1, optimizationOrder.size(), coord));
        double currParamValue = parameterSettings.get(coord); // Keep around the current parameter value
        // Take a step to the right 
        double step = this.minStepSizes.get(coord);
        if (parameterSettings.get(coord) != 0
                && step > (this.maxStepRatio * Math.abs(parameterSettings.get(coord)))) {
          // Reduce the step size for very small weights
          step = (this.maxStepRatio * Math.abs(parameterSettings.get(coord)));
        }
        double rightBest = best;
        double rightStep = 0;
        boolean improving = true;

        while (improving) {
          double curr = parameterSettings.get(coord);
          parameterSettings.unsafeSet(coord, curr + step);
          double evaluation = evaluate(parameterSettings);
          outputTraceStream.println(String.format("Coordinate (%s) ++%f... Metric: %f.", coord, step, evaluation));
          // while we are improving, or equal to the current best - 
          if (evaluation > rightBest || evaluation == best) {
            rightBest = evaluation;
            rightStep += step;
            step *= stepScale;
            // avoid REALLY BIG steps
            if (step > this.MAX_STEP) {
              improving = false;
            }
          } else {
            improving = false;
          }
        }

        // revert changes
        parameterSettings.unsafeSet(coord, currParamValue);

        // Take a step to the right 
        step = this.minStepSizes.get(coord);
        if (parameterSettings.get(coord) != 0
                && step > (this.maxStepRatio * Math.abs(parameterSettings.get(coord)))) {
          // Reduce the step size for very small weights
          step = (this.maxStepRatio * Math.abs(parameterSettings.get(coord)));
        }
        double leftBest = best;
        double leftStep = 0;
        improving = true;
        while (improving) {
          double curr = parameterSettings.get(coord);
          parameterSettings.unsafeSet(coord, curr - step);
          double evaluation = evaluate(parameterSettings);
          outputTraceStream.println(String.format("Coordinate (%s) --%f... Metric: %f.", coord, step, evaluation));
          if (evaluation > leftBest || evaluation == best) {
            leftBest = evaluation;
            leftStep += step;
            step *= stepScale;
            // avoid REALLY BIG steps
            if (step > this.MAX_STEP) {
              improving = false;
            }
          } else {
            improving = false;
          }
        }

        // revert changes
        parameterSettings.unsafeSet(coord, currParamValue);

        // pick a direction to move this parameter
        if ((rightBest > leftBest && rightBest > best) || rightBest > best) {
          optimized = true;
          double curr = parameterSettings.get(coord);
          parameterSettings.unsafeSet(coord, curr + rightStep);
          best = rightBest;
          outputTraceStream.println(String.format("Finished optimizing coordinate (%s). ++%f. Metric: %f", coord, rightStep, best));

        } else if ((leftBest > rightBest && leftBest > best) || leftBest > best) {
          optimized = true;
          double curr = parameterSettings.get(coord);
          parameterSettings.unsafeSet(coord, curr - leftStep);
          best = leftBest;
          outputTraceStream.println(String.format("Finished optimizing coordinate (%s). --%f. Metric: %f", coord, leftStep, best));

        } else {
          outputTraceStream.println(String.format("Finished optimizing coordinate (%s). No Change. Best: %f", coord, best));
        }

        parameterSettings.normalize();
        outputTraceStream.println(String.format("Current source weights: %s", parameterSettings.toString()));
      }
      outputTraceStream.println(String.format("Finished coordinate sweep."));
    }

    outputTraceStream.println(String.format("No changes in the current round or maximum number of iterations reached... Done optimizing."));
    outputTraceStream.println(String.format("Best metric achieved: %s", best));
    return parameterSettings;
  }
}
