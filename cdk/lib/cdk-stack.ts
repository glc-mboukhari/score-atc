import * as cdk from 'aws-cdk-lib';
import { Stack, StackProps } from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { GlcStack, GlcStackProps } from '@groupelacentrale/libs-cdk';

import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
export class CdkStack extends GlcStack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    new s3.Bucket(this, 'datascience-testatc-1', {
      versioned: true,
    });
  }
}
