# sdc_aws_utils Python Package Library

[![Release](https://img.shields.io/github/v/release/HERMES-SOC/sdc_aws_utils)](https://img.shields.io/github/v/release/HERMES-SOC/sdc_aws_utils)
[![Build status](https://img.shields.io/github/actions/workflow/status/HERMES-SOC/sdc_aws_utils/main.yml?branch=main)](https://github.com/HERMES-SOC/sdc_aws_utils/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/HERMES-SOC/sdc_aws_utils/branch/main/graph/badge.svg)](https://codecov.io/gh/HERMES-SOC/sdc_aws_utils)
[![Commit activity](https://img.shields.io/github/commit-activity/m/HERMES-SOC/sdc_aws_utils)](https://img.shields.io/github/commit-activity/m/HERMES-SOC/sdc_aws_utils)
[![License](https://img.shields.io/github/license/HERMES-SOC/sdc_aws_utils)](https://img.shields.io/github/license/HERMES-SOC/sdc_aws_utils)

This is a Python package library that includes utility functions used throughout the different pipeline repos.

- **Github repository**: <https://github.com/HERMES-SOC/sdc_aws_utils/>

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Structure](#structure)
- [Contributing](#contributing)
- [License](#license)

## Introduction

The `sdc_aws_utils` library provides utility functions for working with AWS services like S3 and Timestream, Slack notifications, logging, and pipeline repository configurations. It is designed to be a simple-to-use and efficient library to enhance your projects that rely on AWS services and improve the overall project management experience.

## Installation

To install the `sdc_aws_utils` library directly from the GitHub repository, run the following command:

```bash
pip install git+https://github.com/HERMES-SOC/sdc_aws_utils.git
```

## Structure

The `sdc_aws_utils` library is organized as follows:

```
sdc_aws_utils/
├── aws.py          # Functions for working with AWS services (S3, Timestream)
├── config.py       # Configuration handling
├── __init__.py     # Initialization
├── logging.py      # Logging setup and utilities
└── slack.py        # Functions for working with Slack notifications
```

## Contributing

We welcome contributions to the `sdc_aws_utils` library. Please read the [contributing guidelines](CONTRIBUTING.rst) for more information on how to get involved.

## License

This project is licensed under the [MIT License](LICENSE). For more information, please see the [license file](LICENSE).