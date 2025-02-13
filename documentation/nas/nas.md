# Computing on NAS Pleiades

## Contents

- [Introduction](#Introduction)
- [Storage](#Storage)
- [Conda on NAS](#Conda-on-NAS)
- [Installs](#Installing-Modules-and-Packages)
- [Interactive Sessions](#Interactive-Sessions)
- [Reserved Notes](#Reserved-Notes)
- [SSH](#SSH)
- [Jupyter Notebooks on NAS](#Jupyter-Notebooks-on-NAS)
- [PBS](#PBS)

## Introduction

![alt text](./nas_server_structure.jpg)

This project has been engineered to work well on NASA's NEX Super Computers.

1. For detailed documentation on a number of topics and bugs at NAS please see [here](https://www.nas.nasa.gov/hecc/support/kb/).
1. For an overview of many common NAS tasks see [here](https://www.nas.nasa.gov/hecc/support/kb/basic-tasks_264.html).
1. For an overview of NAS Compute Architecture see [here](https://www.nas.nasa.gov/hecc/support/kb/hpc-environment-overview_25.html).
1. Support can be reached easily by phone at 800-331-8737 and <support@nas.nasa.gov>.

## Storage

1. All data should be stored on Lou. For more information please see [here](https://www.nas.nasa.gov/hecc/support/kb/the-lou-mass-storage-system_371.html).
1. For all data storage purposes, please store your data on `Lou:/u/your_nas_username`
1. `/nobackup/your_nas_username` is cleared regularly. `home4/your_nas_username` is not.
1. NAS has much of the GOES16 data we are working with available in `nex/datapool/goes16`, accessible from pfe.
1. The `/nasa` folder available via pfe has a lot of useful software you can easily load.

## Conda

1. Conda is natively supported on NAS. On a given instance, to ensure that conda is loaded, please call:
`% module use -a /u/analytix/tools/modulefiles` and `% module load miniconda3/v4`.
1. You can create your own conda environments easily, and call them on any interactive or reserved node.

## Installing Modules and Packages

It appears you cannot install python libraries on a compute instance, it needs to be on a pfe.

## Interactive Sessions

All users of NAS can launch an interactive PBS session with qsub, with a sample command found here:
`qsub -I -q v100 -l select=1:ncpus=1:model=sky_gpu,walltime=12:00:00`. The node ID (for instance `r101i0n4`) is constant
per user (it does not change per different interactive sessions).

Alternatively, users can launch jobs via a PBS script. For more information please see [here](https://www.nas.nasa.gov/hecc/support/kb/using-conda-environments-for-machine-learning_557.html).
A sample script can be found in the examples folder as well.

## Reserved Notes

All users can reserve a compute node. For more information see [here](https://www.nas.nasa.gov/hecc/support/kb/reserving-a-dedicated-compute-node_556.html).
These reserved nodes are nice ways to set up long-running Jupyter Notebooks. Users may have at most one reserved node on the Pleaides compute cluster.

To get information about the status of your reserved nodes, use: `pbs_rfe status`.
To delete a reservation, use: `pbs_rfe delete R3286638` where `R3286638` is your reservation ID.

## SSH

When setting up your NAS environment, please setup:

1. [Public Key Authentication](https://www.nas.nasa.gov/hecc/support/kb/setting-up-public-key-authentication_230.html)
for all pfes/sfes you intend to use (sfeX). Note that you do not need to this for all `lou` hosts, which is just storage.
1. [SSH Passthrough.](https://www.nas.nasa.gov/hecc/support/kb/setting-up-ssh-passthrough_232.html)
This repository has an example `~/ssh/config`, including commonly needed ciphers. Importantly, you should setup
SSH passthrough for lou, pfes, and sfes you intend to use. You will not be able to connect to a reserved node or an
interactive node if you do not have passthrough setup for that pfe. You will know passthrough is setup when you are not
asked for your NAS password (only the PASSCODE and RSA Key).
1. When setting up an SSH Tunnel, please use: `ssh -L 5901:node_name:5901 pfe` where `node_name` is the name of the
interactive or reserved node.

## Jupyter Notebooks

1. Please walk through [Secure Jupyter Setup.](https://www.nas.nasa.gov/hecc/support/kb/secure-setup-for-using-jupyter-notebook-on-hecc-systems_576.html)
The instructions in the `Setting Up Jupyter` section are meant to be run on `sfe`, which contain the `/nasa`
folder. You are required to setup a password for a Jupyter notebook.
1. After, please navigate to [Using Jupyter for Machine Learning.](https://www.nas.nasa.gov/hecc/support/kb/using-jupyter-notebook-for-machine-learning_602.html)
1. It's easiest to deploy Jupyter from either a reserved or interactive node. You will need to make sure the relevant
modules are loaded, and the wanted conda environment activated, before launching.
1. The command to connect to the reserved/interactive node is: `your_local_system% ssh -o "StrictHostKeyChecking ask" -L 18080:localhost:8888
-o ProxyJump=sfe,pfe20 r601i0n0`, and the connection will be at: `https://localhost:18080/lab?`. You will need to enter
the password for the Jupyter browser you created earlier.

## PBS

1. PBS jobs can be inspected using the `qstat -u <nas_username>` command.
1. To delete a job please use: `qdel <job_id>`.
