import pprint

import izaber.plpython.base

class IPLPY(izaber.plpython.base.IPLPY):
    def info(self, *args):
        self.plpy.info(*args)

    def rounding(self, f, r):
        if not r:
            return f
        return round(f / r) * r

    def table_exists(self, table_name):
        """ Returns True/False depending on if the table exists
        """
        result = self.q("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_tables
                WHERE schemaname = 'public'
                AND tablename = '{table_name}'
            );
        """.format(table_name=table_name))
        return result[0]['exists']

    def install(self):
        """ Sets up the requisite tables and such in the database
        """
        if not self.table_exists('zerp_product_dirty_log'):

            # Ensure our base table is present
            self.q("""
                CREATE TABLE IF NOT EXISTS zerp_product_dirty_log (
                      id serial primary key,
                      product_id integer not null,
                      update_time timestamp not null,
                      dirty boolean not null,
                      cached_qty_available numeric,
                      cached_virtual_available numeric,
                      cached_incoming_qty numeric,
                      cached_outgoing_qty numeric
                )
            """)

            # Ensure we've got some data
            self.q("""
                INSERT INTO zerp_product_dirty_log
                        (
                            product_id, update_time, dirty,
                            cached_qty_available,
                            cached_virtual_available,
                            cached_incoming_qty,
                            cached_outgoing_qty
                        )
                SELECT
                        id, now(), True,
                        0, 0, 0, 0
                FROM
                        product_product;
            """)

            # Setup the index that allows fast lookup for the latest
            # entries
            self.q("""
                CREATE INDEX ndx_zerp_product_dirty_log_update
                ON           zerp_product_dirty_log
                            ( product_id, update_time desc, dirty desc );
            """)

            # Request that the quantities be recalculated now
            self.sync_product_product_summary();

            # Provide methods to tell the system to sync up data
            self.q("""
                CREATE OR REPLACE FUNCTION fn_sync_product_product_summary()
                RETURNS TEXT AS
                $$
                    from izaber.plpython.zerp import init_plpy
                    iplpy = init_plpy(globals())
                    return iplpy.sync_product_product_summary()
                $$
                LANGUAGE plpython3u;
            """)
            self.q("""
                CREATE OR REPLACE FUNCTION fn_sync_product_product_summary(ids integer[])
                RETURNS TEXT AS
                $$
                    from izaber.plpython.zerp import init_plpy
                    iplpy = init_plpy(globals())
                    return iplpy.sync_product_product_summary(ids)
                $$
                LANGUAGE plpython3u;
            """)


            # Ensure we can vacuum the database of too many entries
            self.q("""
                CREATE OR REPLACE FUNCTION fn_zerp_plpy_vacuum()
                RETURNS TEXT AS
                $$
                    from izaber.plpython.zerp import init_plpy
                    iplpy = init_plpy(globals())
                    return iplpy.vacuum()
                $$
                LANGUAGE plpython3u
            """)



        return "Installed!"

    def vacuum(self):
        """ Cleans up the database. This may be slow so be careful when
            this is called. Also call this when things are quiet since
            this might cause concurrency issues during busy times.
        """

        # Delete all but the most recent entries from the log
        self.q("""
            DELETE FROM zerp_product_dirty_log
            WHERE
                    id not in (
                        select distinct on (product_id) id
                        from
                            zerp_product_dirty_log
                        order by product_id, update_time desc, dirty desc
                    )
        """)

    def get_uom_data(self, uom_id):
        """ Fetches basic information from DB about product.uom
            Caches as needed
            TODO: Cache expiry when DB changes
        """
        if uom_id not in self.GD.setdefault('uom_data_cache',{}):
            data = self.q("""
            SELECT
                    id,
                    category_id,
                    factor,
                    rounding
            FROM
                    product_uom
            WHERE
                    id = $1
            """,["int"],[uom_id])

            self.GD['uom_data_cache'][uom_id] = data and data[0] or None

        return self.GD['uom_data_cache'][uom_id]

    def uom_convert(self, from_uom_id, qty, to_uom_id ):
        """ Replication of the Zerp's UoM conversion function that
            takes one UoM amount to a another UoM based upon the
            conversion amount
        """
        # Just basics where if not defined, just move along
        if not from_uom_id \
           or not qty \
           or not to_uom_id \
           or from_uom_id == to_uom_id:
                return qty

        # FIXME: this is the best way?
        from_unit = self.get_uom_data(from_uom_id)
        to_unit = self.get_uom_data(to_uom_id)

        if not ( from_unit and to_unit ):
            raise Exception("Unknown UoM IDs specified")

        # Guard clause, we don't want to try and convert from a length
        # to a weight, for instance
        if from_unit['category_id'] != to_unit['category_id']:
            raise Exception('Conversion from Product UoM to Default UoM is not possible as they both belong to different Category!.')

        # Calculate the ratios and such
        amount = qty / from_unit['factor']
        amount = self.rounding(amount * to_unit['factor'], to_unit['rounding'])

        return amount

    def get_stock_locations(self):
        """ Returns a list of stock.location ids reflecting locations we consider
            "within Zaber" for the purposes of calulating values such as QoH
        """

        if 'stock_internal_locations' not in self.GD:
            # FIXME: This should be cached
            # Find all the warehouse locations
            sl_rec = self.q("""
                            SELECT      sw.id "warehouse_id",
                                        sl.id "location_id",
                                        sl.parent_left,
                                        sl.parent_right
                            FROM        stock_warehouse sw
                            LEFT JOIN   stock_location sl
                            ON          sw.lot_stock_id = sl.id
                        """,[],[])[0]

            # find the locations associated
            sl_recs = self.q("""
                          SELECT        id
                          FROM          stock_location
                          WHERE
                                        parent_left < $1
                                    AND parent_right >= $2
                        """,
                        ["integer","integer"],
                        [sl_rec['parent_right'], sl_rec['parent_left']])
            sl_ids = []
            for sl_rec in sl_recs:
                sl_ids.append(sl_rec['id'])
            self.GD['stock_internal_locations'] = sl_ids

        return self.GD['stock_internal_locations']

    def get_products_available(self, product_ids):
        """ Returns a hash of product quantities available
            We sum all the in/out moves as two different queries. Note that
            we have to handle UoM after the fact as the database doesn't
            necessarily know how to multiply/etc
        """

        internal_location_ids = tuple(self.get_stock_locations())
        product_id_list = tuple(map(int,product_ids))

        counts = self.q("""
            SELECT
                                c.product_id,
                                direction,
                                SUM(fn_uom_convert(c.product_uom,c.product_qty,pt.uom_id)) product_qty,
                                c.state
            FROM (
                                -- This query takes the various states and sums up the values
                                -- of all the stock move lines
                                select
                                        SUM(product_qty) product_qty,
                                        CASE
                                            WHEN location_id NOT IN ({internal_location_ids})
                                                THEN 'in'
                                            ELSE 'out'
                                        END direction,
                                        product_id,
                                        product_uom,
                                        state
                                from
                                        stock_move
                                where
                                    (( -- incoming moves
                                                location_id NOT IN ({internal_location_ids})
                                            AND location_dest_id IN ({internal_location_ids})
                                        )
                                        OR
                                     ( -- outgoing moves
                                                location_id IN ({internal_location_ids})
                                            AND location_dest_id NOT IN ({internal_location_ids})
                                    ))
                                    and product_id IN ({product_ids})
                                    and state IN ('confirmed','waiting','assigned','done')
                                group by
                                    product_id,
                                    product_uom,
                                    direction,
                                    state
                            ) as c
                LEFT JOIN
                            product_product pp
                        ON  pp.id = c.product_id
                LEFT JOIN
                            product_template pt
                        ON  pt.id = pp.product_tmpl_id
            GROUP BY
                c.product_id, c.direction, c.state
        """.format(
            product_ids=",".join(map(str,product_id_list)),
            internal_location_ids=",".join(map(str,internal_location_ids)),
        ))


        by_product_id = {}
        for product_id in product_id_list:
            by_product_id[product_id] = {
                                            'qty_available': 0,
                                            'virtual_available': 0,
                                            'incoming_qty': 0,
                                            'outgoing_qty': 0,
                                        }

        for count in counts:
            product_id = count['product_id']
            product_result = by_product_id[product_id]

            if count['direction'] == 'in':
                quantity = count['product_qty']
            else:
                quantity = -count['product_qty']

            if count['state'] == 'done':
                product_result['qty_available'] += quantity
                product_result['virtual_available'] += quantity

            else:
                product_result['virtual_available'] += quantity
                if count['direction'] == 'out':
                    product_result['outgoing_qty'] += quantity
                else:
                    product_result['incoming_qty'] += quantity

        return by_product_id

    def get_product_available(self, product_id):
        results = self.get_products_available([product_id])
        return pprint.pformat(results[product_id])

    def sync_product_product_summary(self,ids=None):
        """ Clean up any dirty entries found in the summary table
            If ids are provided, we focus on just those ids. If not,
            we look at all the entries
        """

        # Deal with the dirty product counts
        # We will process in batches to reduce memory impact
        where_cond = ''
        if ids:
            where_cond = 'AND product_id in ({})'.format(",".join(map(str,ids)))
        cur = self.plpy.cursor("""
                    SELECT  product_id
                    FROM (
                            SELECT DISTINCT ON (product_id) product_id, dirty
                            FROM zerp_product_dirty_log
                            ORDER BY product_id, update_time desc, dirty desc
                        ) a
                    WHERE
                            dirty = 't'
                            {where_cond}
                    ;
                """.format(where_cond=where_cond))
        while True:
            rows = cur.fetch(100)
            if not rows:
                break
            self.plpy.info("Syncing {} record(s)...".format(len(rows)))
            product_ids = list(map(lambda a:a['product_id'], rows))
            product_counts = self.get_products_available(product_ids)
            self.GD['product_counts'] = product_counts

            for product_id,vals in product_counts.items():
                self.q("""
                    INSERT INTO zerp_product_dirty_log
                            (
                                product_id, update_time, dirty,
                                cached_qty_available,
                                cached_virtual_available,
                                cached_incoming_qty,
                                cached_outgoing_qty
                            )
                    VALUES  (
                                {product_id}, now(), 'f',
                                {qty_available},
                                {virtual_available},
                                {incoming_qty},
                                {outgoing_qty}
                    )
                """.format(
                    product_id=product_id,
                    **vals
                ))

        return "OK"

    def mark_products_dirty(self,dirty_product_ids):
        if not dirty_product_ids:
            return
        for product_id in dirty_product_ids:
            self.q("""
                INSERT INTO zerp_product_dirty_log
                        (
                            product_id, update_time, dirty,
                            cached_qty_available,
                            cached_virtual_available,
                            cached_incoming_qty,
                            cached_outgoing_qty
                        )
                        VALUES
                        (
                            {product_id}, now(), True,
                            0, 0, 0, 0
                        )
            """.format(
                product_id=product_id
            ))


    def trigger_stock_move_changes(self):
        """ This trigger should execute when a stock.move is created.
            The purpose of this function is to flag in the
            product_product table what records will need quantity
            recalculated
        """
        old = self.TD['old'] or {}
        new = self.TD['new'] or {}
        dirty_product_ids = []
        for product_id in [ old.get('product_id'), new.get('product_id') ]:
            if not product_id: continue
            dirty_product_ids.append(product_id)
        self.mark_products_dirty(dirty_product_ids)

    def trigger_location_changes(self):
        """ This trigger should execute when a location is changed
            The purpose of this function is to flag in the
            product_product table what records will need quantity
            recalculated
        """
        old = self.TD['old'] or {}
        new = self.TD['new'] or {}

        # Go through the stock_move list for any changes that might
        data = self.q("""
            SELECT  DISTINCT product_id
            FROM    stock_move
            WHERE
                    location_id = {location_id}
                OR  location_dest_id = {location_id}
        """.format(
            location_id = old.get('id')
        ))

        dirty_product_ids = []
        for row in data:
            product_id = row['product_id']
            if not product_id: continue
            dirty_product_ids.append(product_id)
        self.mark_products_dirty(dirty_product_ids)

    def trigger_uom_changes(self):
        """ This trigger should execute when a uom is changed
            The purpose of this function is to flag in the
            product_product table what records will need quantity
            recalculated
        """
        old = self.TD['old'] or {}
        new = self.TD['new'] or {}

        # Go through the stock_move list for any changes that might
        data = self.q("""
            SELECT  DISTINCT product_id
            FROM    stock_move
            WHERE
                    product_uom = {uom_id}
        """.format(
            uom_id = old.get('id')
        ))

        dirty_product_ids = []
        for row in data:
            product_id = row['product_id']
            if not product_id: continue
            dirty_product_ids.append(product_id)
        self.mark_products_dirty(dirty_product_ids)

    def trigger_product_changes(self):
        """ This trigger should execute when a product is changed.
            The purpose of this function is to flag in the
            product_product table what records will need quantity
            recalculated
        """
        old = self.TD['old'] or {}
        new = self.TD['new'] or {}
        dirty_product_ids = []
        for product_id in [ old.get('id'), new.get('id') ]:
            if not product_id: continue
            dirty_product_ids.append(product_id)
        self.mark_products_dirty(dirty_product_ids)
"""

CREATE OR REPLACE FUNCTION fn_uom_convert(from_uom_id integer,qty numeric,to_uom_id integer)
RETURNS NUMERIC AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals(),reload=True)
    return iplpy.uom_convert(from_uom_id,qty,to_uom_id)
$$
LANGUAGE plpython3u;
select fn_uom_convert(37,1,1);

CREATE OR REPLACE FUNCTION fn_get_stock_locations()
RETURNS INT[] AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.get_stock_locations()
$$
LANGUAGE plpython3u;
select fn_get_stock_locations();

CREATE OR REPLACE FUNCTION fn_get_product_available(product_id integer)
RETURNS TEXT AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.get_product_available(product_id)
$$
LANGUAGE plpython3u;
select fn_get_product_available(1633);

CREATE OR REPLACE FUNCTION fn_trigger_stock_move_changes()
RETURNS TRIGGER AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.trigger_stock_move_changes()
$$
LANGUAGE plpython3u;

CREATE TRIGGER      trig_stock_move_qty_changes_update
BEFORE UPDATE OF    product_uom,
                    product_qty,
                    location_id,
                    location_dest_id,
                    product_id,
                    state
ON
                    stock_move
FOR EACH ROW
EXECUTE PROCEDURE   fn_trigger_stock_move_changes()
;

CREATE TRIGGER      trig_stock_move_qty_changes_insdel
BEFORE INSERT OR DELETE
ON
                    stock_move
FOR EACH ROW
EXECUTE PROCEDURE   fn_trigger_stock_move_changes()
;

DROP FUNCTION fn_get_cached_available_qty(product_id integer);
CREATE OR REPLACE FUNCTION fn_get_cached_available_qty(product_id integer)
RETURNS NUMERIC AS
$$
    return GD['product_counts'].get(product_id,{}).get('qty_available')
$$
LANGUAGE plpython3u;

DROP FUNCTION fn_get_cached_virtual_available(product_id integer);
CREATE OR REPLACE FUNCTION fn_get_cached_virtual_available(product_id integer)
RETURNS NUMERIC AS
$$
    return GD['product_counts'].get(product_id,{}).get('virtual_available')
$$
LANGUAGE plpython3u;

DROP FUNCTION fn_get_cached_incoming_qty(product_id integer);
CREATE OR REPLACE FUNCTION fn_get_cached_incoming_qty(product_id integer)
RETURNS NUMERIC AS
$$
    return GD['product_counts'].get(product_id,{}).get('incoming_qty')
$$
LANGUAGE plpython3u;

DROP FUNCTION fn_get_cached_outgoing_qty(product_id integer);
CREATE OR REPLACE FUNCTION fn_get_cached_outgoing_qty(product_id integer)
RETURNS NUMERIC AS
$$
    return GD['product_counts'].get(product_id,{}).get('outgoing_qty')
$$
LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION fn_sync_product_product_summary()
RETURNS TEXT AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.sync_product_product_summary()
$$
LANGUAGE plpython3u;


CREATE OR REPLACE FUNCTION fn_trigger_location_changes()
RETURNS TRIGGER AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.trigger_location_changes()
$$
LANGUAGE plpython3u;

CREATE TRIGGER      trig_location_changes_updel
BEFORE UPDATE OF    active,
                    location_id,
ON
                    stock_location
FOR EACH ROW
EXECUTE PROCEDURE   fn_trigger_location_changes()
;


CREATE OR REPLACE FUNCTION fn_trigger_uom_changes()
RETURNS TRIGGER AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.trigger_uom_changes()
$$
LANGUAGE plpython3u;

CREATE TRIGGER      trig_uom_changes_updel
BEFORE UPDATE OF    category_id,
                    factor,
                    rounding
ON
                    product_uom
FOR EACH ROW
EXECUTE PROCEDURE   fn_trigger_uom_changes()
;




CREATE OR REPLACE FUNCTION fn_trigger_product_changes()
RETURNS TRIGGER AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.trigger_product_changes()
$$
LANGUAGE plpython3u;

CREATE TRIGGER      trig_product_changes_updel
BEFORE UPDATE OF    uom_id
ON
                    product_template
FOR EACH ROW
EXECUTE PROCEDURE   fn_trigger_product_changes()
;


CREATE OR REPLACE FUNCTION fn_table_exists(table_name text)
RETURNS BOOLEAN AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.table_exists(table_name)
$$
LANGUAGE plpython3u;


CREATE OR REPLACE FUNCTION fn_zerp_plpy_install()
RETURNS TEXT AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.install()
$$
LANGUAGE plpython3u;


CREATE OR REPLACE FUNCTION fn_zerp_plpy_vacuum()
RETURNS TEXT AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.vacuum()
$$
LANGUAGE plpython3u;



CREATE OR REPLACE FUNCTION fn_zerp_plpy_test(ids integer[])
RETURNS TEXT AS
$$
    plpy.info(ids)
$$
LANGUAGE plpython3u;


"""
