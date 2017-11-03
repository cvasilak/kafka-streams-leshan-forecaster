/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cvasilak.leshan.kafka.streams.utils;

import org.cvasilak.leshan.avro.model.AvroResource;
import org.cvasilak.leshan.avro.response.AvroReadResponse;
import org.cvasilak.leshan.avro.response.AvroResponse;

public class Utils {

    public static AvroResource getResourceFromResponse(AvroResponse response) {
        AvroReadResponse read = (AvroReadResponse) response.getRep().getBody();
        AvroResource resource = (AvroResource) read.getContent();
        return resource;
    }

    public static Double getValueFromResponse(AvroResponse response) {
        AvroResource resource = getResourceFromResponse(response);
        Object val = resource.getValue();
        if (val instanceof Long) {
            return ((Long) val).doubleValue();
        }
        return (Double) val;
    }
}
