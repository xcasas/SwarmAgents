# MIT License
#
# Copyright (c) 2024 swarm-workflows

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
# File: swarm/comm/grpc_consensus_server.py
import logging

import grpc
from concurrent import futures
import json
import asyncio

from swarm.comm import consensus_pb2_grpc, consensus_pb2
from swarm.comm import policy_pb2_grpc, policy_pb2
from swarm.comm.observer import Observer


class ConsensusServiceServicer(consensus_pb2_grpc.ConsensusServiceServicer):
    def __init__(self, observer: Observer):
        self.observer = observer

    def SendMessage(self, request, context):
        msg = {
            "sender_id": request.sender_id,
            "receiver_id": request.receiver_id,
            "message_type": request.message_type,
            "payload": json.loads(request.payload),
            "timestamp": request.timestamp
        }
        self.observer.process_message(msg)
        return consensus_pb2.Ack(success=True, info="Processed")


class PolicyServiceServicer(policy_pb2_grpc.PolicyServiceServicer):
    def __init__(self, observer: Observer):
        self.observer = observer

    def Initialize(self, request, context):
        self.policy_name = request.policy_name
        print(f"Initializing policy: {self.policy_name}")
        return policy_pb2.InitializeOrStopResponse(success=True, message=f"Policy '{self.policy_name}' initialized.")

    def Stop(self, request, context):
        print(f"Stopping policy: {request.policy_name}")
        return policy_pb2.InitializeOrStopResponse(success=True, message=f"Policy '{request.policy_name}' stopped.")

    class PolicyServiceServicer(policy_pb2_grpc.PolicyServiceServicer):
        def __init__(self, observer: Observer):
            self.observer = observer

    # Xavi: this function is used when calling the swarm consensus algorithm directly as a policy.
    def Decide(self, request, context):
        levels = [
            IndicatorLevel(ind.name, ind.value, ind.isMet, ind.threshold, ind.associatedRole)
            for ind in request.levels
        ] if request.levels else []

        roles = {
            role.roleName: [Resource(res.name, res.value) for res in role.resources]
            for role in request.roles
        }

        decisions = {}
        if any(not level.is_met for level in levels):
            self.observer.start_consensus(roles)

            result = asyncio.run(wait_for_decisions(self.observer))

            if result is None:
                self.observer.remove_jobs(roles)
                return policy_pb2.DecideResponse(decisions={})

            decisions = result

            for role in request.roles:
                if role.isRunning:
                    decisions[role.roleName] = True

        return policy_pb2.DecideResponse(decisions=decisions)

    # Xavi: this function is triggered to request the execution or termination of roles. This is called from a policy.
    def RequestRoles(self, request, context):
        roleName = request.role.roleName
        startOrStop = request.startOrStop
        if startOrStop:
            jobName = f"start{roleName}"
        else:
            jobName = f"stop{roleName}"
        resources = request.role.resources


        self.observer.start_consensus({jobName: resources}, startOrStop)

        # Xavi: TODO improve this and make asynchronous calls instead of waiting.
        result = asyncio.run(wait_for_decisions(self.observer, jobName))

        if result is None:
            #print(f"Didn't reach consensus for role {jobName}.")
            self.observer.remove_job(jobName)
            self.observer.pop_decision(jobName)
            return policy_pb2.RoleResponse(reachedConsensus=False, toExecute=request.role.isRunning)

        else:
            #print(f"Reached consensus for role {jobName}.")

            r = request.role.isRunning
            s = startOrStop
            res = result[jobName]

            if s :
                if res:
                    decision = True
                else:
                    decision = r
            else:
                if res:
                    decision = False
                else:
                    decision = r

            # Determine whether a role should be running in the next state, based on:
            # - its current running status (`isRunning`)
            # - whether we're in "start" mode (`startOrStop == True`) or "stop" mode (`startOrStop == False`)
            # - whether this specific role is selected in the `result` map
            #
            # Rules:
            # - If a role is already running and we're starting, keep it running.
            # - If a role is not running but selected to start, start it.
            # - If a role is not running and not selected, keep it stopped.
            # - If a role is running and we're stopping, only stop it if it's selected to stop.
            # - If a role is running and not selected to stop, keep it running.
            self.observer.remove_job(jobName)
            self.observer.pop_decision(jobName)
            return policy_pb2.RoleResponse(reachedConsensus=True, toExecute=decision,
                                           usage=self.observer.calculate_usage())


async def wait_for_decisions(observer, jobName, timeout=10, interval=1):
    total_wait = 0
    while total_wait < timeout:
        decisions = observer.get_decisions()
        if len(decisions) > 0 and jobName in decisions:
            return decisions
        await asyncio.sleep(interval)
        total_wait += interval
    return None


class Resource:
    def __init__(self, name, value):
        self.name = name
        self.value = value


class IndicatorLevel:
    def __init__(self, name, value, is_met, threshold, associated_role):
        self.name = name
        self.value = value
        self.is_met = is_met
        self.threshold = threshold
        self.associated_role = associated_role


class GrpcServer:
    def __init__(self, port: int, observer: Observer, logger: logging.Logger = logging.getLogger()):
        self.port = port
        self.observer = observer
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self._bind_services()
        self.logger = logger

    def _bind_services(self):
        consensus_pb2_grpc.add_ConsensusServiceServicer_to_server(
            ConsensusServiceServicer(self.observer), self.server)
        policy_pb2_grpc.add_PolicyServiceServicer_to_server(PolicyServiceServicer(self.observer), self.server)
        self.server.add_insecure_port(f"[::]:{self.port}")

    def start(self):
        self.logger.info(f"[gRPC] Server starting on port {self.port}...")
        self.server.start()

    def wait_for_termination(self):
        self.server.wait_for_termination()

    def stop(self, grace: int = 1):
        self.logger.info(f"[gRPC] Server shutting down...")
        self.server.stop(grace)
