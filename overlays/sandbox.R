library(sf)

# create a polygon of 1x1 square
m = rbind(c(0,0), c(1,0), c(1,1), c(0,1), c(0,0))
p = st_polygon(list(m))

# generate n squares within the bigger square (aka parcels)
n = 5
set.seed(131)
l = vector("list", n)
for (i in 1:n)
    l[[i]] = p + 5 * runif(2)
s1 = st_sfc(l)
area1 <- st_area(s1)

# generate another n squares that overlap with the other ones (aka env feature)
set.seed(567)
l = vector("list", n)
for (i in 1:n)
    l[[i]] = p + 4 * runif(2)
s2 = st_sfc(l)

# plot results in three plots
par(mfrow = c(1, 3))

# plot the two layers
plot(s1, col = "yellow")
plot(s2, col = "lightblue", add = TRUE)

# compute and show intersections
i = st_intersection(s1, s2) 
plot(s1, col = "yellow")
plot(s2, col = "lightblue", add = TRUE)
plot(i, col = "pink", add = TRUE)
# next the pink areas will be cut out

# compute and show the parcel layer minus the env feature layer
d = st_difference(s1, st_union(s2))
plot(d, col = "orange")

area2 <- st_area(d)

cat("\nArea reduced from", sum(area1), "to", sum(area2), "\n")

