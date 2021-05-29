CREATE TABLE CarLocStr
(
    car_id  INT, /* unique car identifier       */
    speed   INT, /* speed of the car            */
    exp_way INT, /* expressway: 0..10           */
    lane    INT, /* lane: 0,1,2,3               */
    dir     INT, /* direction: 0(east), 1(west) */
    x-pos FLOAT  /* coordinate in express way   */
);

CREATE VIEW CarSegStr
AS
SELECT car_id, speed, exp_way, lane, dir, (x - pos / 52800) as seg
FROM CarLocStr;

CREATE VIEW SegAvgSpeed
AS
SELECT exp_way, dir, seg, AVG(speed) as speed,
FROM CarSegStr [RANGE 5 MINUTES]
GROUP BY exp_way, dir, seg;

CREATE VIEW SegVol
AS
SELECT exp_way, dir, seg, COUNT(*) as volume
FROM CurCarSeg
GROUP BY exp_way, dir, seg;

-- EQUAL JOIN AND AGGREGATION.
SELECT S.exp_way, S.dir, S.seg, basetoll * (V.volume - 150) * (V.volume - 150)
FROM SegAvgSpeed as S,
     SegVol as V
WHERE S.exp_way = V.exp_way
  and S.dir = V.dir
  and S.seg = V.seg
  and S.speed < 40
  and (S.exp_way, S.dir, S.seg) NOT IN (AccAffectedSeg);