﻿using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Xunit;

namespace FastTests.Server.Documents.Queries.Dynamic.Map
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    public class DynamicQueriesEnumsNestedFieldsAndCollections : RavenNewTestBase
    {
        [Fact]
        public async Task Query_on_enum()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company()
                    {
                        Type = Company.CompanyType.Private
                    }, "companies/1");

                    await session.StoreAsync(new Company()
                    {
                        Type = Company.CompanyType.Public
                    }, "companies/2");

                    await session.StoreAsync(new Company()
                    {
                        Type = Company.CompanyType.Private
                    }, "companies/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var privateCompanies = session.Query<Company>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Type == Company.CompanyType.Private).ToList();

                    Assert.Equal(2, privateCompanies.Count);
                    Assert.Equal("companies/1", privateCompanies[0].Id);
                    Assert.Equal("companies/3", privateCompanies[1].Id);
                }
            }
        }

        [Fact]
        public async Task Query_on_nested_field()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order()
                    {
                        ShipTo = new Address()
                        {
                            City = "Torun"
                        }
                    }, "orders/1");

                    await session.StoreAsync(new Order()
                    {
                        ShipTo = new Address()
                        {
                            City = "Gdansk"
                        }
                    }, "orders/2");

                    await session.StoreAsync(new Order()
                    {
                        ShipTo = new Address()
                        {
                            City = "Torun"
                        }
                    }, "orders/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Order>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.ShipTo.City == "Torun").ToList();

                    Assert.Equal(2, orders.Count);
                    Assert.Equal("orders/1", orders[0].Id);
                    Assert.Equal("orders/3", orders[1].Id);
                }
            }
        }

        [Fact]
        public async Task Query_on_collection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order()
                    {
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine()
                            {
                                ProductName = "Keyboard"
                            },
                            new OrderLine()
                            {
                                PricePerUnit = 12
                            }
                        }
                    }, "orders/1");

                    await session.StoreAsync(new Order()
                    {
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine()
                            {
                                ProductName = "Microphone"
                            },
                            new OrderLine()
                            {
                                PricePerUnit = 10
                            }
                        }
                    }, "orders/2");

                    await session.StoreAsync(new Order()
                    {
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine()
                            {
                                ProductName = "Mouse"
                            },
                            new OrderLine()
                            {
                                ProductName = "Keyboard"
                            },
                            new OrderLine()
                            {
                                PricePerUnit = 9
                            }
                        }
                    }, "orders/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Order>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Lines.Any(y => y.ProductName == "Keyboard")).ToList();

                    Assert.Equal(2, orders.Count);
                    Assert.Equal("orders/1", orders[0].Id);
                    Assert.Equal("orders/3", orders[1].Id);

                    orders = session.Query<Order>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Lines.Any(y => y.PricePerUnit >= 10)).ToList();

                    Assert.Equal(2, orders.Count);
                    Assert.Equal("orders/1", orders[0].Id);
                    Assert.Equal("orders/2", orders[1].Id);
                }
            }
        }
    }
}