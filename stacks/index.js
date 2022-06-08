import MyStack from "./MyStack";
import { Runtime, Tracing } from "aws-cdk-lib/aws-lambda";
import { Duration } from 'aws-cdk-lib';

export default function main(app) {
  // Set default runtime for all functions
  app.setDefaultFunctionProps({
    srcPath: 'src',
    runtime: Runtime.PYTHON_3_8,
    tracing: Tracing.ACTIVE,
    timeout: Duration.seconds(30),
  });

  // Define sst-stack
  new MyStack(app, "sst-stack");
}
