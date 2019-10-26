--
-- Created by IntelliJ IDEA.
-- User: igorlo
-- Date: 05.10.2019
-- Time: 0:10
--

counter = 0

request = function()
    path = "/v0/entity?id=key" .. counter .. "&replicas=3/3"
    wrk.method = "PUT"
    wrk.body = "value" .. counter
    counter = counter + 1
    return wrk.format(nil, path)
end
