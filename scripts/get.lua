--
-- Created by IntelliJ IDEA.
-- User: igorlo
-- Date: 05.10.2019
-- Time: 0:02
--


counter = 0

request = function()
    path = "/v0/entity?id=key" .. counter
    wrk.method = "GET"
    counter = counter + 1
    return wrk.format(nil, path)
end
