#Simple Python OPC-UA Server
#Sending out 2 data values
#Flo Pachinger / flopach, Cisco Systems, July 2020
#Script based on the server example https://github.com/FreeOpcUa/python-opcua
#LGPL-3.0 License

import logging
import asyncio
import pandas as pd

from asyncua import ua, Server
from asyncua.common.methods import uamethod
from asyncua.ua import VariantType

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('asyncua')

@uamethod
def func(parent, value):
    return value * 2

async def main():
    # setup our server
    server = Server()
    await server.init()
    server.set_endpoint('opc.tcp://127.0.0.1:4840/opcua/')
    server.set_server_name("Exos Solutions OPC-UA Test Server")

    # setup our own namespace, not really necessary but should as spec
    uri = 'http://exos-solutions.com/opcua/'
    idx = await server.register_namespace(uri)

    # populating our address space
    # server.nodes, contains links to very common nodes like objects and root
    obj_vplc = await server.nodes.objects.add_object(idx, 'vPLC1')
    # var_temperature = await obj_vplc.add_variable(idx, 'temperature', 0, varianttype=VariantType.Double)
    # var_pressure = await obj_vplc.add_variable(idx, 'pressure', 0, varianttype=VariantType.Double)
    # var_pumpsetting = await obj_vplc.add_variable(idx, 'pumpsetting', 0, varianttype=VariantType.String)
    var_estado = await obj_vplc.add_variable(idx, 'estado', 0, varianttype=VariantType.Boolean);
    var_produccion = await obj_vplc.add_variable(idx, 'produccion', 0, varianttype=VariantType.Int64)

    # Read Sensor Data from Kaggle
    df = pd.read_csv("sensorExos.csv")
    # Only use sensor data from 03 and 01 (preference)
    sensor_data = pd.concat([df["sensor_produccion"], df["sensor_estados"]], axis=1)

    _logger.info('Starting server!')
    async with server:
        # run forever and iterate over the dataframe
        while True:
            for row in sensor_data.itertuples():
                # if below the mean use different setting - just for testing
                # if row[1] < df["sensor_03"].mean():
                #     setting = "standard"
                # else:
                #     setting = "speed"

                # Writing Variables
                # await var_temperature.write_value(float(row[1]))
                # await var_pressure.write_value(float(row[2]))
                # await var_pumpsetting.write_value(str(setting))
                estado = bool(int(row[2]))

                await var_estado.write_value(estado)
                await var_produccion.write_value(int(row[1]))
                await asyncio.sleep(6)

if __name__ == '__main__':
    # python 3.6 or lower
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    #python 3.7 onwards (comment lines above)
    asyncio.run(main())
