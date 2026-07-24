import os
import sys

# cdk/ holds top-level modules (settings, stack, stack_constructs) that are not on
# the default pytest path; add it so the CDK tests can import them.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "cdk"))
