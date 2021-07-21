JPaxos and mPaxos /master/
==========================

Description
-----------

JPaxos is a Java library and runtime system for building efficient replicated
state machines (highly-available services). With JPaxos it is easy to make
a user-provided service which is tolerant to machine crashes. Our system
supports the crash-recovery model of failure and tolerates message loss and
communication delays.

mPaxos builds on JPaxos and offers a programming framework which leverages 
persistent memory (pmem), also known as Non-Volatile Memory (NVM), for building
more performant replicated state machines, which write critical data to pmem
rather than to stable storage. Parts of mPaxos are built in C++ using PMDK
library. For more on pmem, see: 
https://www.snia.org/technology-focus/persistent-memory.

mPaxosSM is a sibling version of mPaxos that requires that also the 
user-provided service writes to pmem. It is available at: 
https://github.com/JPaxos/mPaxosSM.

All our systems are based on solid theoretical foundations, following the 
state-of-the-art in group communication research.

You are free to use our systems as the experimental platforms for doing 
research on software-based replication, or for any other purposes, provided
that the LGPL 3.0 licence is respected, moreover, any research papers which
are presenting the results obtained with the use of our systems must reference
the appropriate articles enlisted in the LICENCE file and below.

Research Papers
---------------

* Failure Recovery from Persistent Memory in Paxos-based State Machine 
  Replication. Jan Kończak, Paweł T. Wojciechowski. 40th International 
  Symposium on Reliable Distributed Systems (SRDS 2021).

* Recovery Algorithms for Paxos-based State Machine Replication. Jan Kończak,
  Paweł T. Wojciechowski, Nuno Santos, Tomasz Żurkowski and André Schiper.
  IEEE Transactions on Dependable and Secure Computing (TPDS), 
  Volume: 18, Issue: 2, March-April 1, 2021, available at: 
  https://ieeexplore.ieee.org/abstract/document/8758801

* JPaxos: State Machine Replication Based on the Paxos Protocol. Jan Kończak,
  Nuno Santos, Tomasz Żurkowski, Paweł T. Wojciechowski and André Schiper.
  Technical Report 167765, EPFL, July 2011, available at: 
  https://infoscience.epfl.ch/record/167765.

Version
-------

The gitub repository may not contain the most recent version of our systems.
Please query the authors for more recent code, especially if the last commit
is far in the past.

License
-------

This software is distributed under the LGPL 3.0 licence. For license details
please read the LICENCE file.

Contact and authors
-------------------

JPaxos was a joint work between the Distributed System Laboratory (LSR-EPFL)
and Poznan University of Technology (PUT). It has been further developed by
Poznan University of Technology. mPaxos has been developed at PUT.

Institutional pages and contact details:

* EPFL: http://archiveweb.epfl.ch/lsrwww.epfl.ch/
* PUT:  http://www.cs.put.poznan.pl/persistentdatastore/

Contributors:

At EPFL-LSR:

* Andre Schiper (no longer active)
* Nuno Santos (no longer active)
* Lisa Nguyen Quang Do (no longer active)

At PUT:

* Jan Kończak
* Maciej Kokociński
* Tadeusz Kobus
* Paweł T. Wojciechowski
* Tomasz Żurkowski (no longer active)
